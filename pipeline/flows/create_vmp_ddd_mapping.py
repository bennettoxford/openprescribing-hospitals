from prefect import flow, get_run_logger
from pipeline.utils.utils import execute_bigquery_query_from_sql_file
from pathlib import Path


@flow(name="Create VMP DDD mapping")
def create_vmp_ddd_mapping():
    logger = get_run_logger()
    logger.info("Creating VMP DDD mapping")

    sql_file_path = Path(__file__).parent.parent / "sql" / "create_vmp_ddd_mapping.sql"

    execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD mapping calculated")


if __name__ == "__main__":
    create_vmp_ddd_mapping()
