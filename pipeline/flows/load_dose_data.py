import argparse
import time
import pandas as pd
from google.cloud import bigquery
from prefect import get_run_logger, task, flow
from django.db import transaction
from typing import Dict, List, Tuple
from pipeline.bq_tables import DOSE_TABLE_SPEC
from pipeline.utils.utils import setup_django_environment, get_bigquery_client


setup_django_environment()
from viewer.models import Dose, SCMDQuantity, VMP, Organisation


def ensure_proper_types(data_list):
    """Ensure each entry in the data array has the proper types"""
    for i, entry in enumerate(data_list):
        date_val = entry[0]
        if not isinstance(date_val, str):
            data_list[i][0] = str(date_val)

        qty_val = entry[1]
        if not isinstance(qty_val, float):
            data_list[i][1] = float(qty_val)

        unit_val = entry[2]
        if not isinstance(unit_val, str):
            data_list[i][2] = str(unit_val)

    return data_list


@task
def get_unique_vmps_with_dose_data() -> List[str]:
    """Get all unique VMP codes that have dose or SCMD data"""
    logger = get_run_logger()
    client = get_bigquery_client()

    query = f"""
    SELECT DISTINCT vmp_code
    FROM `{DOSE_TABLE_SPEC.full_table_id}`
    WHERE (dose_quantity IS NOT NULL AND dose_unit IS NOT NULL)
       OR (scmd_quantity IS NOT NULL AND scmd_basis_unit_name IS NOT NULL)
    ORDER BY vmp_code
    """

    result = client.query(query).to_dataframe()
    vmp_codes = result["vmp_code"].tolist()
    logger.info(f"Found {len(vmp_codes):,} unique VMPs with dose/SCMD data")
    return vmp_codes


@task
def extract_dose_data_by_vmps(
    vmp_codes: List[str], chunk_num: int, total_chunks: int
) -> pd.DataFrame:
    """Extract dose and SCMD data for a specific set of VMP codes"""
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
        dose_quantity,
        dose_unit,
        scmd_quantity,
        scmd_basis_unit_name
    FROM `{DOSE_TABLE_SPEC.full_table_id}`
    WHERE ((dose_quantity IS NOT NULL AND dose_unit IS NOT NULL)
           OR (scmd_quantity IS NOT NULL AND scmd_basis_unit_name IS NOT NULL))
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
def clear_existing_dose_data() -> Tuple[int, int]:
    """Clear all existing dose and SCMD quantity data in chunks"""
    logger = get_run_logger()
    logger.info("Clearing existing dose and SCMD quantity data")

    total_dose_deleted = 0
    total_scmd_deleted = 0
    chunk_size = 10_000

    while True:
        with transaction.atomic():
            batch_ids = list(Dose.objects.values_list("id", flat=True)[:chunk_size])
            if not batch_ids:
                break

            deleted_count = Dose.objects.filter(id__in=batch_ids).delete()[0]
            total_dose_deleted += deleted_count

    while True:
        with transaction.atomic():
            batch_ids = list(
                SCMDQuantity.objects.values_list("id", flat=True)[:chunk_size]
            )
            if not batch_ids:
                break

            deleted_count = SCMDQuantity.objects.filter(id__in=batch_ids).delete()[0]
            total_scmd_deleted += deleted_count

    logger.info(f"Deleted {total_dose_deleted:,} existing dose records")
    logger.info(f"Deleted {total_scmd_deleted:,} existing SCMD quantity records")
    return total_dose_deleted, total_scmd_deleted


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
        f"Chunk {chunk_num}/{total_chunks}: Transforming and loading {len(chunk_df):,} records"
    )

    if len(chunk_df) == 0:
        logger.info(f"Chunk {chunk_num}/{total_chunks}: No data to process")
        return {"dose_created": 0, "scmd_created": 0, "skipped": 0}

    logger.info(f"Chunk {chunk_num}/{total_chunks}: Filtering for valid records...")
    valid_mask = (
        chunk_df["vmp_code"].notna()
        & chunk_df["ods_code"].notna()
        & chunk_df["year_month"].notna()
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
        return {"dose_created": 0, "scmd_created": 0, "skipped": skipped_count}

    logger.info(f"Chunk {chunk_num}/{total_chunks}: Converting data types...")
    df_valid["year_month"] = pd.to_datetime(df_valid["year_month"]).dt.strftime(
        "%Y-%m-%d"
    )

    logger.info(f"Chunk {chunk_num}/{total_chunks}: Grouping data...")
    dose_data = {}
    scmd_data = {}

    for (vmp_code, ods_code), group in df_valid.groupby(["vmp_code", "ods_code"]):
        dose_group = group[
            (group["dose_quantity"].notna()) & (group["dose_unit"].notna())
        ]
        if len(dose_group) > 0:
            dose_array = []
            for _, row in dose_group.iterrows():
                dose_array.append(
                    [
                        row["year_month"],
                        float(row["dose_quantity"]),
                        str(row["dose_unit"]),
                    ]
                )
            dose_data[(vmp_code, ods_code)] = ensure_proper_types(dose_array)

        scmd_group = group[
            (group["scmd_quantity"].notna()) & (group["scmd_basis_unit_name"].notna())
        ]
        if len(scmd_group) > 0:
            scmd_array = []
            for _, row in scmd_group.iterrows():
                scmd_array.append(
                    [
                        row["year_month"],
                        float(row["scmd_quantity"]),
                        str(row["scmd_basis_unit_name"]),
                    ]
                )
            scmd_data[(vmp_code, ods_code)] = ensure_proper_types(scmd_array)

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Created {len(dose_data):,} dose combinations and {len(scmd_data):,} SCMD combinations"
    )

    vmps = foreign_key_cache["vmps"]
    organisations = foreign_key_cache["organisations"]

    dose_objects = []
    dose_skipped = 0

    for (vmp_code, ods_code), data_array in dose_data.items():
        if vmp_code in vmps and ods_code in organisations:
            dose_objects.append(
                Dose(
                    vmp_id=vmps[vmp_code],
                    organisation_id=organisations[ods_code],
                    data=data_array,
                )
            )
        else:
            dose_skipped += 1

    scmd_objects = []
    scmd_skipped = 0

    for (vmp_code, ods_code), data_array in scmd_data.items():
        if vmp_code in vmps and ods_code in organisations:
            scmd_objects.append(
                SCMDQuantity(
                    vmp_id=vmps[vmp_code],
                    organisation_id=organisations[ods_code],
                    data=data_array,
                )
            )
        else:
            scmd_skipped += 1

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Loading {len(dose_objects):,} dose objects and {len(scmd_objects):,} SCMD objects to database..."
    )

    SUB_BATCH_SIZE = 1000
    dose_created = 0
    scmd_created = 0
    total_skipped = skipped_count + dose_skipped + scmd_skipped

    for i in range(0, len(dose_objects), SUB_BATCH_SIZE):
        sub_batch = dose_objects[i:i + SUB_BATCH_SIZE]

        try:
            with transaction.atomic():
                Dose.objects.bulk_create(
                    sub_batch,
                    batch_size=SUB_BATCH_SIZE,
                    unique_fields=["vmp", "organisation"],
                )
                dose_created += len(sub_batch)

        except Exception as e:
            logger.error(
                f"Chunk {chunk_num}/{total_chunks}: Error in dose sub-batch {i//SUB_BATCH_SIZE + 1}: {str(e)}"
            )
            total_skipped += len(sub_batch)

    for i in range(0, len(scmd_objects), SUB_BATCH_SIZE):
        sub_batch = scmd_objects[i:i + SUB_BATCH_SIZE]

        try:
            with transaction.atomic():
                SCMDQuantity.objects.bulk_create(
                    sub_batch,
                    batch_size=SUB_BATCH_SIZE,
                    unique_fields=["vmp", "organisation"],
                )
                scmd_created += len(sub_batch)

        except Exception as e:
            logger.error(
                f"Chunk {chunk_num}/{total_chunks}: Error in SCMD sub-batch {i//SUB_BATCH_SIZE + 1}: {str(e)}"
            )
            total_skipped += len(sub_batch)

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Load complete. Dose created: {dose_created:,}, SCMD created: {scmd_created:,}, Skipped: {total_skipped:,}"
    )

    return {
        "dose_created": dose_created,
        "scmd_created": scmd_created,
        "skipped": total_skipped,
    }


@flow
def load_dose_data_flow(vmp_chunk_size: int = 1000):
    """
    Main flow to import dose and SCMD quantity data using VMP-based chunking

    Args:
        vmp_chunk_size: Number of VMPs to process in each chunk (default: 1000)
    """
    logger = get_run_logger()
    start_time = time.time()

    logger.info(f"Starting dose data import with VMP-based chunking")

    all_vmps = get_unique_vmps_with_dose_data()
    total_chunks = (len(all_vmps) + vmp_chunk_size - 1) // vmp_chunk_size
    logger.info(
        f"Will process {len(all_vmps):,} VMPs in {total_chunks} chunks of {vmp_chunk_size} VMPs each"
    )

    dose_deleted, scmd_deleted = clear_existing_dose_data()
    foreign_key_cache = cache_foreign_keys()

    total_stats = {
        "total_dose_created": 0,
        "total_scmd_created": 0,
        "total_skipped": 0,
        "total_processed_chunks": 0,
    }

    for chunk_num in range(1, total_chunks + 1):
        chunk_start_time = time.time()
        start_idx = (chunk_num - 1) * vmp_chunk_size
        end_idx = min(start_idx + vmp_chunk_size, len(all_vmps))
        chunk_vmps = all_vmps[start_idx:end_idx]
        chunk_df = extract_dose_data_by_vmps(chunk_vmps, chunk_num, total_chunks)

        if len(chunk_df) == 0:
            logger.info(
                f"Chunk {chunk_num}: No data returned for VMPs {start_idx+1}-{end_idx}"
            )
            continue

        chunk_result = transform_and_load_chunk(
            chunk_df, foreign_key_cache, chunk_num, total_chunks
        )

        total_stats["total_dose_created"] += chunk_result["dose_created"]
        total_stats["total_scmd_created"] += chunk_result["scmd_created"]
        total_stats["total_skipped"] += chunk_result["skipped"]
        total_stats["total_processed_chunks"] += 1

        chunk_duration = time.time() - chunk_start_time

        progress_pct = (chunk_num / total_chunks) * 100

        logger.info(
            f"Chunk {chunk_num}/{total_chunks} complete in {chunk_duration:.1f}s ({progress_pct:.1f}% done). "
            f"Processed VMPs {start_idx+1}-{end_idx}. "
            f"Running totals - Dose created: {total_stats['total_dose_created']:,}, "
            f"SCMD created: {total_stats['total_scmd_created']:,}, Skipped: {total_stats['total_skipped']:,}. "
        )

    total_time = time.time() - start_time

    logger.info(
        f"Dose data import completed in {total_time/60:.1f} minutes. "
        f"Dose deleted: {dose_deleted:,}, Dose created: {total_stats['total_dose_created']:,}, "
        f"SCMD deleted: {scmd_deleted:,}, SCMD created: {total_stats['total_scmd_created']:,}, "
        f"Skipped: {total_stats['total_skipped']:,}, Chunks processed: {total_stats['total_processed_chunks']}. "
    )

    return {
        "dose_deleted": dose_deleted,
        "dose_created": total_stats["total_dose_created"],
        "scmd_deleted": scmd_deleted,
        "scmd_created": total_stats["total_scmd_created"],
        "skipped": total_stats["total_skipped"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load dose and SCMD quantity data using VMP-based chunking"
    )
    parser.add_argument(
        "--vmp-chunk-size",
        type=int,
        default=1000,
        help="Number of VMPs per chunk (default: 1000)",
    )

    args = parser.parse_args()

    load_dose_data_flow(vmp_chunk_size=args.vmp_chunk_size)
