import argparse
import time
import pandas as pd
from google.cloud import bigquery
from prefect import get_run_logger, task, flow
from django.db import transaction
from typing import Dict, List, Tuple
from pipeline.bq_tables import DDD_QUANTITY_TABLE_SPEC, CALCULATION_LOGIC_TABLE_SPEC
from pipeline.utils.utils import setup_django_environment, get_bigquery_client


setup_django_environment()
from viewer.models import DDDQuantity, VMP, Organisation, CalculationLogic


@task
def get_ddd_calculation_logic() -> Dict[str, str]:
    """Download calculation logic for DDD calculations from the calculation logic table"""
    logger = get_run_logger()
    client = get_bigquery_client()

    query = f"""
    SELECT 
        vmp_code,
        logic
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type = 'ddd'
    """

    result = client.query(query).to_dataframe(create_bqstorage_client=True)
    
    logic_dict = dict(zip(result['vmp_code'], result['logic']))
    
    logger.info(f"Downloaded DDD calculation logic for {len(logic_dict):,} VMPs")
    return logic_dict


@task
def get_unique_vmps_with_ddd_data() -> List[str]:
    """Get all unique VMP codes that have DDD quantity data"""
    logger = get_run_logger()
    client = get_bigquery_client()

    query = f"""
    SELECT DISTINCT vmp_code
    FROM `{DDD_QUANTITY_TABLE_SPEC.full_table_id}`
    WHERE ddd_quantity IS NOT NULL
    ORDER BY vmp_code
    """

    result = client.query(query).to_dataframe()
    vmp_codes = result["vmp_code"].tolist()
    logger.info(f"Found {len(vmp_codes):,} unique VMPs with DDD quantity data")
    return vmp_codes


@task
def extract_ddd_data_by_vmps(
    vmp_codes: List[str], chunk_num: int, total_chunks: int
) -> pd.DataFrame:
    """Extract DDD quantity data for a specific set of VMP codes"""
    logger = get_run_logger()

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Extracting data for {len(vmp_codes):,} VMPs"
    )

    client = get_bigquery_client()

    vmp_list_str = "', '".join(vmp_codes)

    query = f"""
    SELECT 
        vmp_code,
        year_month,
        ods_code,
        ddd_quantity,
        ddd_value,
        ddd_unit
    FROM `{DDD_QUANTITY_TABLE_SPEC.full_table_id}`
    WHERE ddd_quantity IS NOT NULL
    AND vmp_code IN ('{vmp_list_str}')
    ORDER BY vmp_code, ods_code, year_month
    """

    job_config = bigquery.QueryJobConfig(
        use_query_cache=False, allow_large_results=True
    )

    query_job = client.query(query, job_config=job_config)
    df = query_job.to_dataframe(create_bqstorage_client=True)

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Extracted {len(df):,} rows for {len(vmp_codes):,} VMPs"
    )

    return df


@task
def clear_existing_ddd_data() -> Tuple[int, int]:
    """Clear all existing DDD quantity data and calculation logic"""
    logger = get_run_logger()
    logger.info("Clearing existing DDD quantity data and calculation logic")

    total_deleted = 0
    total_logic_deleted = 0
    chunk_size = 10_000

    while True:
        with transaction.atomic():
            batch_ids = list(
                CalculationLogic.objects.filter(logic_type='ddd').values_list("id", flat=True)[:chunk_size]
            )
            if not batch_ids:
                break

            deleted_count = CalculationLogic.objects.filter(id__in=batch_ids).delete()[0]
            total_logic_deleted += deleted_count

    while True:
        with transaction.atomic():
            batch_ids = list(
                DDDQuantity.objects.values_list("id", flat=True)[:chunk_size]
            )
            if not batch_ids:
                break

            deleted_count = DDDQuantity.objects.filter(id__in=batch_ids).delete()[0]
            total_deleted += deleted_count

    logger.info(f"Deleted {total_deleted:,} existing DDD quantity records")
    logger.info(f"Deleted {total_logic_deleted:,} existing DDD calculation logic records")
    return total_deleted, total_logic_deleted


@task
def cache_foreign_keys() -> Dict:
    """Cache all VMP and Organisation foreign keys"""
    logger = get_run_logger()
    logger.info("Caching foreign key mappings")

    vmps = {vmp.code: vmp.id for vmp in VMP.objects.all()}
    organisations = {org.ods_code: org.id for org in Organisation.objects.all()}

    logger.info(f"Cached {len(vmps):,} VMPs and {len(organisations):,} organisations")
    return {"vmps": vmps, "organisations": organisations}


@task
def load_ddd_logic(
    chunk_df: pd.DataFrame, 
    foreign_key_cache: Dict, 
    ddd_logic_dict: Dict[str, str],
    chunk_num: int, 
    total_chunks: int
) -> Dict:
    """Load DDD logic using the centralized calculation logic"""
    logger = get_run_logger()
    
    if len(chunk_df) == 0:
        return {"logic_created": 0, "logic_conflicts": 0}
    
    chunk_vmps = chunk_df['vmp_code'].unique()
    
    vmps = foreign_key_cache["vmps"]
    logic_objects = []
    logic_conflicts = 0
    logic_created = 0
    
    for vmp_code in chunk_vmps:
        if vmp_code in ddd_logic_dict and vmp_code in vmps:
            logic_objects.append(
                CalculationLogic(
                    vmp_id=vmps[vmp_code],
                    logic_type='ddd',
                    logic=ddd_logic_dict[vmp_code],
                    ingredient=None
                )
            )
        elif vmp_code not in ddd_logic_dict:
            logger.warning(f"Chunk {chunk_num}/{total_chunks}: No DDD logic found for VMP {vmp_code}")
    
    if logic_objects:
        try:
            with transaction.atomic():
                CalculationLogic.objects.bulk_create(logic_objects)
                logic_created = len(logic_objects)
        except Exception as e:
            logger.error(f"Chunk {chunk_num}/{total_chunks}: Error creating DDD logic: {str(e)}")
    
    logger.info(f"Chunk {chunk_num}/{total_chunks}: Created {logic_created} DDD logic records")
    
    return {"logic_created": logic_created, "logic_conflicts": logic_conflicts}


@task
def transform_and_load_chunk(
    chunk_df: pd.DataFrame,
    foreign_key_cache: Dict,
    ddd_logic_dict: Dict[str, str],
    chunk_num: int,
    total_chunks: int,
) -> Dict:
    """Transform and load a chunk from BigQuery DDD data"""
    logger = get_run_logger()

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Transforming and loading {len(chunk_df):,} records"
    )

    if len(chunk_df) == 0:
        logger.info(f"Chunk {chunk_num}/{total_chunks}: No data to process")
        return {"created": 0, "skipped": 0, "logic_created": 0, "logic_conflicts": 0}

    logic_result = load_ddd_logic(chunk_df, foreign_key_cache, ddd_logic_dict, chunk_num, total_chunks)

    valid_mask = (
        chunk_df["vmp_code"].notna()
        & chunk_df["ods_code"].notna()
        & chunk_df["year_month"].notna()
        & chunk_df["ddd_quantity"].notna()
    )

    df_valid = chunk_df[valid_mask].copy()
    skipped_count = len(chunk_df) - len(df_valid)

    if skipped_count > 0:
        logger.info(
            f"Chunk {chunk_num}/{total_chunks}: Filtered out {skipped_count:,} records with missing data"
        )

    if len(df_valid) == 0:
        logger.info(f"Chunk {chunk_num}/{total_chunks}: No valid data to process after filtering")
        return {"created": 0, "skipped": skipped_count, **logic_result}

    df_valid["year_month"] = pd.to_datetime(df_valid["year_month"]).dt.strftime("%Y-%m-%d")

    grouped_data = {}
    for (vmp_code, ods_code), group in df_valid.groupby(["vmp_code", "ods_code"]):
        data_array = []
        for row in group.itertuples(index=False):
            ddd_unit_desc = f"DDD ({row.ddd_value} {row.ddd_unit})" if pd.notna(row.ddd_value) and pd.notna(row.ddd_unit) else "DDD"
            data_array.append([
                row.year_month,
                str(float(row.ddd_quantity)),
                ddd_unit_desc
            ])

        if (vmp_code in foreign_key_cache["vmps"] and 
            ods_code in foreign_key_cache["organisations"]):
            key = (
                foreign_key_cache["vmps"][vmp_code],
                foreign_key_cache["organisations"][ods_code],
            )
            grouped_data[key] = data_array

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Prepared {len(grouped_data)} VMP-organisation combinations"
    )

    ddd_objects = []
    for (vmp_id, org_id), data_array in grouped_data.items():
        ddd_objects.append(
            DDDQuantity(vmp_id=vmp_id, organisation_id=org_id, data=data_array)
        )

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Loading {len(ddd_objects):,} objects to database..."
    )

    SUB_BATCH_SIZE = 1000
    total_created = 0
    total_skipped = skipped_count

    for i in range(0, len(ddd_objects), SUB_BATCH_SIZE):
        sub_batch = ddd_objects[i : i + SUB_BATCH_SIZE]

        try:
            with transaction.atomic():
                DDDQuantity.objects.bulk_create(
                    sub_batch,
                    batch_size=SUB_BATCH_SIZE,
                    unique_fields=["vmp", "organisation"],
                )
                total_created += len(sub_batch)

        except Exception as e:
            logger.error(
                f"Chunk {chunk_num}/{total_chunks}: Error in sub-batch {i//SUB_BATCH_SIZE + 1}: {str(e)}"
            )
            total_skipped += len(sub_batch)

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Load complete. Created: {total_created:,}, Skipped: {total_skipped:,}"
    )

    return {
        "created": total_created,
        "skipped": total_skipped,
        **logic_result
    }


@flow
def load_ddd_quantity_flow(vmp_chunk_size: int = 1000):
    """
    Main flow to import DDD quantity data from BigQuery

    Args:
        vmp_chunk_size: Number of VMPs to process in each chunk (default: 1000)
    """
    logger = get_run_logger()
    start_time = time.time()

    logger.info(f"Starting DDD quantity data import from BigQuery")

    ddd_logic_dict = get_ddd_calculation_logic()

    all_vmps = get_unique_vmps_with_ddd_data()

    total_chunks = (len(all_vmps) + vmp_chunk_size - 1) // vmp_chunk_size
    logger.info(
        f"Will process {len(all_vmps):,} VMPs in {total_chunks} chunks of {vmp_chunk_size} VMPs each"
    )

    deleted_count, logic_deleted_count = clear_existing_ddd_data()
    foreign_key_cache = cache_foreign_keys()

    total_stats = {
        "total_created": 0,
        "total_skipped": 0,
        "total_logic_created": 0,
        "total_logic_conflicts": 0,
        "total_processed_chunks": 0,
    }

    for chunk_num in range(1, total_chunks + 1):
        chunk_start_time = time.time()

        start_idx = (chunk_num - 1) * vmp_chunk_size
        end_idx = min(start_idx + vmp_chunk_size, len(all_vmps))
        chunk_vmps = all_vmps[start_idx:end_idx]

        chunk_df = extract_ddd_data_by_vmps(chunk_vmps, chunk_num, total_chunks)

        chunk_result = transform_and_load_chunk(
            chunk_df, foreign_key_cache, ddd_logic_dict, chunk_num, total_chunks
        )

        total_stats["total_created"] += chunk_result["created"]
        total_stats["total_skipped"] += chunk_result["skipped"]
        total_stats["total_logic_created"] += chunk_result["logic_created"]
        total_stats["total_logic_conflicts"] += chunk_result["logic_conflicts"]
        total_stats["total_processed_chunks"] += 1

        chunk_duration = time.time() - chunk_start_time
        progress_pct = (chunk_num / total_chunks) * 100

        logger.info(
            f"Chunk {chunk_num}/{total_chunks} complete in {chunk_duration:.1f}s ({progress_pct:.1f}% done). "
            f"Processed VMPs {start_idx+1}-{end_idx}. "
            f"Running totals - Created: {total_stats['total_created']:,}, Logic created: {total_stats['total_logic_created']:,}, "
            f"Logic conflicts: {total_stats['total_logic_conflicts']:,}, Skipped: {total_stats['total_skipped']:,}. "
        )

    total_time = time.time() - start_time

    logger.info(
        f"DDD quantity data import completed in {total_time/60:.1f} minutes. "
        f"Deleted: {deleted_count:,}, Created: {total_stats['total_created']:,}, "
        f"Logic deleted: {logic_deleted_count:,}, Logic created: {total_stats['total_logic_created']:,}, "
        f"Logic conflicts: {total_stats['total_logic_conflicts']:,}, "
        f"Skipped: {total_stats['total_skipped']:,}, Chunks processed: {total_stats['total_processed_chunks']}. "
    )

    return {
        "deleted": deleted_count,
        "created": total_stats["total_created"],
        "logic_deleted": logic_deleted_count,
        "logic_created": total_stats["total_logic_created"],
        "logic_conflicts": total_stats["total_logic_conflicts"],
        "skipped": total_stats["total_skipped"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load DDD quantity data from BigQuery"
    )
    parser.add_argument(
        "--vmp-chunk-size",
        type=int,
        default=1000,
        help="Number of VMPs per chunk (default: 1000)",
    )

    args = parser.parse_args()

    load_ddd_quantity_flow(vmp_chunk_size=args.vmp_chunk_size)
