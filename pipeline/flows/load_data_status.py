import pandas as pd

from prefect import get_run_logger, task, flow
from django.db import transaction
from typing import Dict
from pipeline.utils.utils import setup_django_environment, fetch_table_data_from_bq
from pipeline.bq_tables import SCMD_DATA_STATUS_TABLE_SPEC


setup_django_environment()

from viewer.models import DataStatus


@task()
def extract_data_status() -> pd.DataFrame:
    """Extract data status information from BigQuery"""
    logger = get_run_logger()
    logger.info("Starting extraction of data status from BigQuery")

    df = fetch_table_data_from_bq(SCMD_DATA_STATUS_TABLE_SPEC, use_bqstorage=False)

    logger.info(f"Extracted {len(df)} records from BigQuery")
    return df


@task
def load_data_status(data: pd.DataFrame) -> Dict:
    """Load data status information by replacing all existing data"""
    logger = get_run_logger()
    logger.info(f"Loading {len(data)} data status records")

    with transaction.atomic():

        deleted_count = DataStatus.objects.all().delete()[0]
        logger.info(f"Deleted {deleted_count} existing records")

        data_status_objects = [
            DataStatus(year_month=row["year_month"], file_type=row["file_type"])
            for row in data.to_dict(orient="records")
        ]

        created_objects = DataStatus.objects.bulk_create(
            data_status_objects, batch_size=1000
        )

        total_created = len(created_objects)

    logger.info(
        f"Data status load complete. Deleted: {deleted_count}, Created: {total_created}"
    )
    return {
        "deleted": deleted_count,
        "created": total_created,
        "total_records": len(data),
    }


@flow(name="Load Data Status")
def load_data_status_flow():
    """Main flow to import data status from BigQuery to Django"""
    logger = get_run_logger()
    logger.info("Starting data status import flow")

    status_data = extract_data_status()

    result = load_data_status(status_data)

    return result


if __name__ == "__main__":
    load_data_status_flow()
