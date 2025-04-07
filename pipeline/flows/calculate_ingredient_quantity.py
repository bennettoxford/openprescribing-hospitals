from prefect import flow, task, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
    validate_table_schema,
)
from pipeline.bq_tables import INGREDIENT_QUANTITY_TABLE_SPEC
from pathlib import Path


@task
def validate_ingredient_quantity():
    """Validate the ingredient quantity data"""
    logger = get_run_logger()
    logger.info("Validating ingredient quantity table schema")

    schema_valid = validate_table_schema(INGREDIENT_QUANTITY_TABLE_SPEC)

    if not schema_valid:
        logger.error("Schema validation failed for ingredient quantity table")
        raise ValueError("Schema validation failed for ingredient quantity table")

    logger.info("Schema validation successful for ingredient quantity table")
    return {"schema_valid": schema_valid}


@flow(name="Calculate ingredient quantities")
def calculate_ingredient_quantity():
    logger = get_run_logger()
    logger.info("Calculating ingredient quantities")

    sql_file_path = (
        Path(__file__).parent.parent / "sql" / "calculate_ingredient_quantity.sql"
    )

    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("Ingredient quantities calculated")

    validation_result = validate_ingredient_quantity()

    return {
        "sql_result": result,
        "validation": validation_result,
        "status": "completed",
    }


if __name__ == "__main__":
    calculate_ingredient_quantity()
