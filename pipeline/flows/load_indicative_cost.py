import time
import argparse
import pandas as pd
from prefect import get_run_logger, task, flow
from django.db import transaction
from typing import Dict, List
from pipeline.bq_tables import SCMD_PROCESSED_TABLE_SPEC
from pipeline.utils.utils import setup_django_environment, get_bigquery_client
from google.cloud import bigquery


setup_django_environment()

from viewer.models import IndicativeCost, VMP, Organisation


@task
def get_unique_vmps() -> List[str]:
    """Get all unique VMP codes that have indicative cost data"""
    logger = get_run_logger()
    client = get_bigquery_client()

    query = f"""
    SELECT DISTINCT vmp_code
    FROM `{SCMD_PROCESSED_TABLE_SPEC.full_table_id}`
    WHERE indicative_cost IS NOT NULL
    ORDER BY vmp_code
    """

    result = client.query(query).to_dataframe()
    vmp_codes = result["vmp_code"].tolist()
    logger.info(f"Found {len(vmp_codes):,} unique VMPs with indicative cost data")
    return vmp_codes


@task
def extract_indicative_cost_by_vmps(
    vmp_codes: List[str], chunk_num: int, total_chunks: int
) -> pd.DataFrame:
    """Extract indicative cost data for a specific set of VMP codes"""
    logger = get_run_logger()

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Extracting data for {len(vmp_codes):,} VMPs"
    )

    client = get_bigquery_client()

    vmp_list_str = "', '".join(vmp_codes)

    query = f"""
    SELECT 
        year_month, 
        vmp_code, 
        ods_code, 
        indicative_cost
    FROM `{SCMD_PROCESSED_TABLE_SPEC.full_table_id}`
    WHERE indicative_cost IS NOT NULL
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
def clear_existing_data() -> int:
    """Clear all existing indicative cost data"""
    logger = get_run_logger()
    logger.info("Clearing existing indicative cost data")

    total_deleted = 0
    chunk_size = 10_000

    while True:
        with transaction.atomic():
            batch_ids = list(
                IndicativeCost.objects.values_list("id", flat=True)[:chunk_size]
            )
            if not batch_ids:
                break

            deleted_count = IndicativeCost.objects.filter(id__in=batch_ids).delete()[0]
            total_deleted += deleted_count

    logger.info(f"Deleted {total_deleted:,} existing indicative cost records")
    return total_deleted


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
def transform_and_load_chunk(
    chunk_df: pd.DataFrame, foreign_key_cache: Dict, chunk_num: int, total_chunks: int
) -> Dict:
    """Transform and load a chunk"""
    logger = get_run_logger()

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Transforming and loading {len(chunk_df):,} records "
    )

    if len(chunk_df) == 0:
        logger.info(f"Chunk {chunk_num}/{total_chunks}: No data to process")
        return {"created": 0, "updated": 0, "skipped": 0}

    logger.info(f"Chunk {chunk_num}/{total_chunks}: Filtering for valid records...")
    valid_mask = (
        chunk_df["vmp_code"].notna()
        & chunk_df["ods_code"].notna()
        & chunk_df["year_month"].notna()
        & chunk_df["indicative_cost"].notna()
    )

    df_valid = chunk_df[valid_mask].copy()
    skipped_count = len(chunk_df) - len(df_valid)

    if skipped_count > 0:
        logger.info(
            f"Chunk {chunk_num}/{total_chunks}: Filtered out {skipped_count:,} records with missing data"
        )

    if len(df_valid) == 0:
        logger.info(
            f"Chunk {chunk_num}/{total_chunks}: No valid records after filtering"
        )
        return {"created": 0, "updated": 0, "skipped": skipped_count}

    df_valid["year_month"] = pd.to_datetime(df_valid["year_month"]).dt.strftime(
        "%Y-%m-%d"
    )
    df_valid["cost_str"] = df_valid["indicative_cost"].round(2).astype(str)

    logger.info(f"Chunk {chunk_num}/{total_chunks}: Grouping data...")
    grouped_data = {}
    for (vmp_code, ods_code), group in df_valid.groupby(["vmp_code", "ods_code"]):
        data_array = list(zip(group["year_month"].tolist(), group["cost_str"].tolist()))
        grouped_data[(vmp_code, ods_code)] = data_array

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Created {len(grouped_data):,} VMP-Organisation combinations"
    )

    vmps = foreign_key_cache["vmps"]
    organisations = foreign_key_cache["organisations"]

    ic_objects = []
    skipped_due_to_missing_fk = 0

    for (vmp_code, ods_code), data_array in grouped_data.items():
        if vmp_code in vmps and ods_code in organisations:
            ic_objects.append(
                IndicativeCost(
                    vmp_id=vmps[vmp_code],
                    organisation_id=organisations[ods_code],
                    data=data_array,
                )
            )
        else:
            skipped_due_to_missing_fk += 1

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Loading {len(ic_objects):,} objects to database..."
    )

    SUB_BATCH_SIZE = 1000
    total_created = 0
    total_updated = 0
    total_skipped = skipped_count + skipped_due_to_missing_fk

    for i in range(0, len(ic_objects), SUB_BATCH_SIZE):
        sub_batch = ic_objects[i : i + SUB_BATCH_SIZE]

        try:
            with transaction.atomic():
                IndicativeCost.objects.bulk_create(
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
        "updated": total_updated,
        "skipped": total_skipped,
    }


@flow
def load_indicative_costs_flow(vmp_chunk_size: int = 1000):
    logger = get_run_logger()
    start_time = time.time()

    logger.info(f"Starting indicative cost data import with VMP-based chunking")

    all_vmps = get_unique_vmps()
    total_chunks = (len(all_vmps) + vmp_chunk_size - 1) // vmp_chunk_size
    logger.info(
        f"Will process {len(all_vmps):,} VMPs in {total_chunks} chunks of {vmp_chunk_size} VMPs each"
    )

    deleted_count = clear_existing_data()
    foreign_key_cache = cache_foreign_keys()

    total_stats = {
        "total_created": 0,
        "total_updated": 0,
        "total_skipped": 0,
        "total_processed_chunks": 0,
    }

    for chunk_num in range(1, total_chunks + 1):
        chunk_start_time = time.time()

        start_idx = (chunk_num - 1) * vmp_chunk_size
        end_idx = min(start_idx + vmp_chunk_size, len(all_vmps))
        chunk_vmps = all_vmps[start_idx:end_idx]

        chunk_df = extract_indicative_cost_by_vmps(chunk_vmps, chunk_num, total_chunks)

        if len(chunk_df) == 0:
            logger.info(
                f"Chunk {chunk_num}: No data returned for VMPs {start_idx+1}-{end_idx}"
            )
            continue

        chunk_result = transform_and_load_chunk(
            chunk_df, foreign_key_cache, chunk_num, total_chunks
        )

        total_stats["total_created"] += chunk_result["created"]
        total_stats["total_updated"] += chunk_result["updated"]
        total_stats["total_skipped"] += chunk_result["skipped"]
        total_stats["total_processed_chunks"] += 1

        chunk_duration = time.time() - chunk_start_time
        progress_pct = (chunk_num / total_chunks) * 100

        logger.info(
            f"Chunk {chunk_num}/{total_chunks} complete in {chunk_duration:.1f}s ({progress_pct:.1f}% done). "
            f"Processed VMPs {start_idx+1}-{end_idx}. "
            f"Running totals - Created: {total_stats['total_created']:,}, Skipped: {total_stats['total_skipped']:,}. "
        )

    total_time = time.time() - start_time

    logger.info(
        f"Indicative cost data import completed in {total_time/60:.1f} minutes. "
        f"Deleted: {deleted_count:,}, Created: {total_stats['total_created']:,}, "
        f"Skipped: {total_stats['total_skipped']:,}, Chunks processed: {total_stats['total_processed_chunks']}. "
    )

    return {
        "deleted": deleted_count,
        "created": total_stats["total_created"],
        "skipped": total_stats["total_skipped"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load indicative cost data using VMP-based chunking"
    )
    parser.add_argument(
        "--vmp-chunk-size",
        type=int,
        default=1000,
        help="Number of VMPs per chunk (default: 1000)",
    )

    args = parser.parse_args()

    load_indicative_costs_flow(vmp_chunk_size=args.vmp_chunk_size)
