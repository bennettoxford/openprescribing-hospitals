from prefect import flow, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
)
from pathlib import Path


@flow(name="Handle DDD route comments")
def handle_ddd_route_comments():
    logger = get_run_logger()

    sql_file_path = (
        Path(__file__).parent / "handle_ddd_route_comments.sql"
    )

    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD route comments applied to DDD calculation logic")


if __name__ == "__main__":
    handle_ddd_route_comments()