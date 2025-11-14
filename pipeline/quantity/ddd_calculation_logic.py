from prefect import flow, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
)
from pathlib import Path



@flow(name="Check DDD calculations")
def ddd_calculation_logic():
    logger = get_run_logger()
    logger.info("Checking DDD calculation logic")

    sql_file_path = (
        Path(__file__).parent / "ddd_calculation_logic.sql"
    )

    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD calculations checked")


if __name__ == "__main__":
    ddd_calculation_logic()
