from prefect import flow, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
)
from pathlib import Path



@flow(name="Check calculations")
def dose_calculation_logic():
    logger = get_run_logger()
    logger.info("Checking dose calculation logic")

    sql_file_path = (
        Path(__file__).parent / "dose_calculation_logic.sql"
    )

    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("Calculations checked")


if __name__ == "__main__":
    dose_calculation_logic()
