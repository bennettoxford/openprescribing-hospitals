from prefect import flow, task, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
    validate_table_schema,
)
from pipeline.bq_tables import DOSE_TABLE_SPEC
from pathlib import Path


@task
def validate_calculated_doses():
    """Validate the calculated doses data"""
    logger = get_run_logger()
    logger.info("Validating calculated doses table schema")

    schema_valid = validate_table_schema(DOSE_TABLE_SPEC)

    if not schema_valid:
        logger.error("Schema validation failed for doses table")
        raise ValueError("Schema validation failed for doses table")

    logger.info("Schema validation successful for doses table")
    return {"schema_valid": schema_valid}


@flow(name="Calculate doses")
def calculate_doses():
    logger = get_run_logger()
    logger.info("Calculating doses")

    sql_file_path = Path(__file__).parent.parent / "sql" / "calculate_doses.sql"
    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("Doses calculated")

    validation_result = validate_calculated_doses()

    return {
        "sql_result": result,
        "validation": validation_result,
        "status": "completed",
    }


if __name__ == "__main__":
    calculate_doses()
