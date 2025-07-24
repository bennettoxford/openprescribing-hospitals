from prefect import flow, get_run_logger
from pipeline.utils.utils import execute_bigquery_query_from_sql_file, validate_table_schema
from pipeline.bq_tables import DDD_REFERS_TO_TABLE_SPEC
from pathlib import Path


@flow(name="Populate DDD Refers To Table")
def populate_ddd_refers_to_table():
    logger = get_run_logger()
    logger.info("Populating DDD Refers To table")

    sql_file_path = Path(__file__).parent.parent / "sql" / "populate_ddd_refers_to.sql"

    sql_result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD Refers To table populated successfully")
    
    schema_validation = validate_table_schema(DDD_REFERS_TO_TABLE_SPEC)
    logger.info("DDD Refers To table schema validation completed")
    
    return {
        "sql_result": sql_result,
        "schema_validation": schema_validation,
    }


if __name__ == "__main__":
    populate_ddd_refers_to_table() 