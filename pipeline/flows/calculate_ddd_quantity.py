from prefect import flow, task, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
    validate_table_schema,
    get_bigquery_client,
)
from pipeline.bq_tables import DDD_QUANTITY_TABLE_SPEC
from pathlib import Path


@task
def validate_ddd_quantity_schema():
    """Validate the DDD quantity data schema"""
    logger = get_run_logger()
    logger.info("Validating DDD quantity table schema")

    schema_valid = validate_table_schema(DDD_QUANTITY_TABLE_SPEC)

    if not schema_valid:
        logger.error("Schema validation failed for DDD quantity table")
        raise ValueError("Schema validation failed for DDD quantity table")

    logger.info("Schema validation successful for DDD quantity table")
    return {"schema_valid": schema_valid}


@task
def validate_ddd_values_present():
    """Validate that records have DDD values and units when they should be calculable"""
    logger = get_run_logger()
    logger.info("Validating DDD values and units are present when calculable")

    client = get_bigquery_client()
    query = f"""
    SELECT
        vmp_code,
        vmp_name,
        ddd_value,
        ddd_unit,
        calculation_explanation
    FROM `{DDD_QUANTITY_TABLE_SPEC.full_table_id}`
    WHERE 
        calculation_explanation LIKE 'DDD calculation%'
        AND (ddd_value IS NULL OR ddd_unit IS NULL)
    GROUP BY vmp_code, vmp_name, ddd_value, ddd_unit, calculation_explanation
    """
    
    results = client.query(query).result()
    errors = list(results)
    
    if errors:
        logger.error(f"Found {len(errors)} VMPs that should have DDD values but have missing values or units:")
        for error in errors[:5]:
            logger.error(
                f"VMP: {error.vmp_code} ({error.vmp_name}), "
                f"DDD value: {error.ddd_value}, "
                f"DDD unit: {error.ddd_unit}"
            )
        raise ValueError("DDD values presence validation failed")
    
    logger.info("All VMPs with calculable DDDs have proper DDD values and units")
    return {"ddd_values_presence_validation": "passed"}


@flow(name="Calculate DDD quantities")
def calculate_ddd_quantity():
    logger = get_run_logger()
    logger.info("Calculating DDD quantities")

    sql_file_path = Path(__file__).parent.parent / "sql" / "calculate_ddd_quantity.sql"

    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD quantities calculated")

    validations = {
        "schema": validate_ddd_quantity_schema(),
        "ddd_values_present": validate_ddd_values_present(),
    }

    return {
        "sql_result": result,
        "validations": validations,
        "status": "completed",
    }


if __name__ == "__main__":
    calculate_ddd_quantity()
