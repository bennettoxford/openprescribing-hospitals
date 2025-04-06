from prefect import flow, task, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
    validate_table_schema,
)
from pipeline.bq_tables import DDD_QUANTITY_TABLE_SPEC
from pathlib import Path


@task
def validate_ddd_quantity():
    """Validate the DDD quantity data"""
    logger = get_run_logger()
    logger.info("Validating DDD quantity table schema")

    schema_valid = validate_table_schema(DDD_QUANTITY_TABLE_SPEC)

    if not schema_valid:
        logger.error("Schema validation failed for DDD quantity table")
        raise ValueError("Schema validation failed for DDD quantity table")

    logger.info("Schema validation successful for DDD quantity table")
    return {"schema_valid": schema_valid}


@flow(name="Calculate DDD quantities")
def calculate_ddd_quantity():
    logger = get_run_logger()
    logger.info("Calculating DDD quantities")

    sql_file_path = Path(__file__).parent.parent / "sql" / "calculate_ddd_quantity.sql"

    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD quantities calculated")

    validation_result = validate_ddd_quantity()

    return {
        "sql_result": result,
        "validation": validation_result,
        "status": "completed",
    }


if __name__ == "__main__":
    calculate_ddd_quantity()
