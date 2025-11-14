from prefect import flow, task, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
    validate_table_schema,
    get_bigquery_client
)
from pipeline.setup.bq_tables import DOSE_TABLE_SPEC
from pathlib import Path


@task
def validate_calculated_doses():
    """Validate the calculated doses data"""
    logger = get_run_logger()
    client = get_bigquery_client()

    logger.info("Validating calculated doses table schema")
    schema_valid = validate_table_schema(DOSE_TABLE_SPEC)

    validation_query = f"""
    SELECT 
        vmp_code,
        vmp_name,
        calculation_logic,
        dose_quantity,
        dose_unit,
        CASE
            WHEN dose_quantity IS NOT NULL AND calculation_logic LIKE 'Not calculated:%' 
                THEN 'Dose calculated despite logic indicating it should not be'
            WHEN dose_quantity IS NULL AND calculation_logic NOT LIKE 'Not calculated:%'
                THEN 'Dose not calculated despite logic indicating it should be'
            WHEN dose_quantity IS NOT NULL AND dose_unit IS NULL
                THEN 'Dose quantity without dose unit'
        END as issue
    FROM `{DOSE_TABLE_SPEC.full_table_id}`
    WHERE 
        (dose_quantity IS NOT NULL AND calculation_logic LIKE 'Not calculated:%')
        OR (dose_quantity IS NULL AND calculation_logic NOT LIKE 'Not calculated:%')
        OR (dose_quantity IS NOT NULL AND dose_unit IS NULL)
    """

    results = client.query(validation_query).result()
    validation_issues = [dict(row) for row in results]

    if validation_issues:
        logger.error(f"Found {len(validation_issues)} validation issues:")
        for issue in validation_issues[:5]:
            logger.error(
                f"- VMP {issue['vmp_code']} ({issue['vmp_name']}): "
                f"{issue['issue']} (Logic: {issue['calculation_logic']})"
            )

    return {
        "schema_valid": schema_valid,
        "validation_issues": validation_issues,
        "valid": len(validation_issues) == 0
    }


@flow(name="Calculate doses")
def calculate_doses():
    logger = get_run_logger()
    logger.info("Calculating doses")

    sql_file_path = Path(__file__).parent / "calculate_doses.sql"
    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("Doses calculated")

    validation_result = validate_calculated_doses()
    if not validation_result["valid"]:
        logger.error("Dose calculation validation failed")
        logger.error(f"Validation results: {validation_result}")
    else:
        logger.info("Dose calculation validation completed successfully")



if __name__ == "__main__":
    calculate_doses()
