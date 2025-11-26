from prefect import flow, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
)
from pathlib import Path


@flow(name="Handle DDD comments for expressed as")
def handle_ddd_comments_expressed_as():
    logger = get_run_logger()
    logger.info("Processing DDD comment handling for expressed as")

    sql_file_path = (
        Path(__file__).parent / "handle_ddd_comments_expressed_as.sql"
    )

    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD comment handling completed")


if __name__ == "__main__":
    handle_ddd_comments_expressed_as()

