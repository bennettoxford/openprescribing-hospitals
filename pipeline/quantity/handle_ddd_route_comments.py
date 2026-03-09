from prefect import flow, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
    get_bigquery_client,
)
from pipeline.setup.bq_tables import DDD_CALCULATION_LOGIC_TABLE_SPEC, DDD_ROUTE_COMMENTS_TABLE_SPEC
from pathlib import Path


def _validate_override_routes_have_route():
    """Raise if any override VMP has a selected DDD value but no selected_ddd_route_code."""
    client = get_bigquery_client()
    query = f"""
        SELECT logic.vmp_code, logic.vmp_name
        FROM `{DDD_CALCULATION_LOGIC_TABLE_SPEC.full_table_id}` logic
        INNER JOIN `{DDD_ROUTE_COMMENTS_TABLE_SPEC.full_table_id}` comments
          ON logic.vmp_code = comments.vmp_code
        WHERE logic.selected_ddd_value IS NOT NULL
          AND logic.selected_ddd_route_code IS NULL
    """
    rows = list(client.query(query).result())
    if rows:
        vmp_list = ", ".join(f"{r.vmp_code} ({r.vmp_name})" for r in rows)
        raise ValueError(
            f"DDD override applied but no route found for: {vmp_list}"
        )


@flow(name="Handle DDD route comments")
def handle_ddd_route_comments():
    logger = get_run_logger()

    sql_file_path = (
        Path(__file__).parent / "handle_ddd_route_comments.sql"
    )

    execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD route comments applied to DDD calculation logic")

    _validate_override_routes_have_route()
    logger.info("Validated: all override rows have route defined")


if __name__ == "__main__":
    handle_ddd_route_comments()