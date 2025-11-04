from prefect import get_run_logger, task, flow
from django.db import transaction
from typing import List, Dict
import pandas as pd
from pipeline.utils.utils import setup_django_environment, fetch_table_data_from_bq
from pipeline.setup.bq_tables import WHO_ATC_TABLE_SPEC

setup_django_environment()
from viewer.models import ATC


@task()
def extract_atc_codes() -> pd.DataFrame:
    """Extract ATC code data from BigQuery"""
    logger = get_run_logger()
    logger.info("Starting extraction of ATC code data from BigQuery")

    df = fetch_table_data_from_bq(WHO_ATC_TABLE_SPEC, use_bqstorage=True)

    df = df[df["atc_code"].notna()]

    logger.info(f"Extracted {len(df)} rows of ATC code data from BigQuery")
    return df


@task
def transform_atc_codes(data: pd.DataFrame) -> List[Dict]:
    """Transform the ATC code data to match Django model structure"""
    logger = get_run_logger()
    logger.info("Transforming ATC code data")

    data_records = data.to_dict("records")

    # Sort by code length to ensure parents are created before children
    data_records.sort(key=lambda x: len(x.get("atc_code", "")))

    transformed_data = []
    for row in data_records:
        if not row.get("atc_code"):
            continue

        code = row["atc_code"].strip()
        name = row.get("atc_name", "").strip() if row.get("atc_name") else ""

        transformed_row = {
            "code": code,
            "name": name,
            "level_1": row.get("anatomical_main_group"),
            "level_2": row.get("therapeutic_subgroup"),
            "level_3": row.get("pharmacological_subgroup"),
            "level_4": row.get("chemical_subgroup"),
            "level_5": row.get("chemical_substance"),
        }

        transformed_data.append(transformed_row)

    logger.info(f"Transformed {len(transformed_data)} ATC code records")
    return transformed_data


@task
def load_atc_codes(data: List[Dict]) -> Dict:
    """Load ATC code data by replacing all existing data"""
    logger = get_run_logger()
    logger.info(f"Loading {len(data)} ATC code records")

    with transaction.atomic():
        deleted_count = ATC.objects.all().delete()[0]
        logger.info(f"Deleted {deleted_count} existing ATC records")

        atc_objects = [
            ATC(
                code=row["code"],
                name=row["name"],
                level_1=row["level_1"],
                level_2=row["level_2"],
                level_3=row["level_3"],
                level_4=row["level_4"],
                level_5=row["level_5"],
            )
            for row in data
        ]

        created_objects = ATC.objects.bulk_create(atc_objects, batch_size=1000)

        total_created = len(created_objects)

    logger.info(
        f"ATC code data load complete. Deleted: {deleted_count}, Created: {total_created}"
    )
    return {
        "deleted": deleted_count,
        "created": total_created,
        "total_records": len(data),
    }


@flow
def load_atc():
    """Main flow to import ATC code data from BigQuery to Django"""
    logger = get_run_logger()
    logger.info("Starting ATC code import flow")

    atc_data = extract_atc_codes()
    transformed_data = transform_atc_codes(atc_data)
    result = load_atc_codes(transformed_data)

    logger.info(
        f"ATC code import complete. Deleted: {result['deleted']}, Created: {result['created']}"
    )


if __name__ == "__main__":
    load_atc()
