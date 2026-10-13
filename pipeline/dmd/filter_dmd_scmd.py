from pathlib import Path

from prefect import flow, get_run_logger

from pipeline.setup.bq_tables import DMD_TABLE_SPEC
from pipeline.utils.utils import execute_bigquery_query_from_sql_file, validate_table_schema


@flow(name="Filter dm+d SCMD")
def filter_dmd_scmd():
    """Create the filtered dm+d table for SCMD products from the full table."""
    logger = get_run_logger()
    logger.info("Creating filtered dm+d table for SCMD products")

    sql_file_path = Path(__file__).parent / "filter_dmd_scmd.sql"
    execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("Filtered dm+d table built")
    if not validate_table_schema(DMD_TABLE_SPEC):
        raise ValueError(f"Schema validation failed for {DMD_TABLE_SPEC.full_table_id}")


if __name__ == "__main__":
    filter_dmd_scmd()
