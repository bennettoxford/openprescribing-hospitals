from prefect import flow, task, get_run_logger
from pathlib import Path
from pipeline.utils.utils import (
    get_bigquery_client,
    execute_bigquery_query_from_sql_file,
    validate_table_schema,
)
from pipeline.bq_tables import DMD_TABLE_SPEC, ADM_ROUTE_MAPPING_TABLE_SPEC


@task
def validate_routes():
    logger = get_run_logger()
    client = get_bigquery_client()

    query = f"""
    WITH unique_routes AS (
        SELECT DISTINCT ontformroute_descr
        FROM `{DMD_TABLE_SPEC.full_table_id}`,
        UNNEST(ontformroutes)
    )
    SELECT ontformroute_descr
    FROM unique_routes
    WHERE ontformroute_descr NOT IN (
        SELECT dmd_ontformroute
        FROM `{ADM_ROUTE_MAPPING_TABLE_SPEC.full_table_id}`
    )
    """

    results = client.query(query).result()
    missing_routes = [row.ontformroute_descr for row in results]

    if missing_routes:
        logger.error(
            f"Found routes in DMD that are not in route mapping table: {missing_routes}"
        )

    logger.info("All DMD routes are present in route mapping table")


@flow(name="Import dm+d")
def import_dmd():
    logger = get_run_logger()
    logger.info("Importing dm+d")

    sql_file_path = Path(__file__).parent.parent / "sql" / "import_dmd.sql"

    sql_result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("dm+d imported")

    validation_result = validate_routes()
    schema_validation = validate_table_schema(DMD_TABLE_SPEC)

    return {
        "sql_result": sql_result,
        "route_validation": validation_result,
        "schema_validation": schema_validation,
    }


if __name__ == "__main__":
    import_dmd()
