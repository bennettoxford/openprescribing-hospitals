from prefect import flow, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
)
from pathlib import Path


@flow(name="Handle DDD comments for refers to")
def ddd_comment_handling_refers_to():
    logger = get_run_logger()
    logger.info("Processing DDD comment handling for refers to")

    sql_file_path = (
        Path(__file__).parent / "ddd_refers_to_calculation_logic.sql"
    )

    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD comment handling completed")


if __name__ == "__main__":
    ddd_comment_handling_refers_to()
