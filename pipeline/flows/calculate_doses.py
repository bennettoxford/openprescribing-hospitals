from prefect import flow, task, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
    validate_table_schema,
    get_bigquery_client
)
from pipeline.bq_tables import DOSE_TABLE_SPEC
from pathlib import Path


@task
def validate_calculated_doses():
    """Validate the calculated doses data"""
    logger = get_run_logger()
    client = get_bigquery_client()

    logger.info("Validating calculated doses table schema")
    schema_valid = validate_table_schema(DOSE_TABLE_SPEC)

    # Check dose form and unit basis consistency
    validation_query = f"""
    SELECT 
        vmp_code,
        vmp_name,
        df_ind,
        scmd_basis_unit_name,
        udfs_basis_uom,
        dose_quantity,
        CASE
            WHEN dose_quantity IS NOT NULL AND df_ind != 'Discrete' 
                THEN 'Non-discrete form with calculated dose'
            WHEN dose_quantity IS NOT NULL AND scmd_basis_unit_name != udfs_basis_uom 
                THEN 'Basis unit mismatch'
        END as issue
    FROM `{DOSE_TABLE_SPEC.full_table_id}`
    WHERE 
        (dose_quantity IS NOT NULL AND df_ind != 'Discrete')
        OR (dose_quantity IS NOT NULL AND scmd_basis_unit_name != udfs_basis_uom)
    """

    results = client.query(validation_query).result()
    validation_issues = [dict(row) for row in results]

    if validation_issues:
        logger.error(f"Found {len(validation_issues)} validation issues:")
        for issue in validation_issues[:5]:
            if issue['df_ind'] != 'Discrete':
                logger.error(
                    f"- VMP {issue['vmp_code']} ({issue['vmp_name']}): "
                    f"Has calculated dose but form is {issue['df_ind']}"
                )
            else:
                logger.error(
                    f"- VMP {issue['vmp_code']} ({issue['vmp_name']}): "
                    f"SCMD basis unit ({issue['scmd_basis_unit_name']}) "
                    f"!= UDFS basis unit ({issue['udfs_basis_uom']})"
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

    sql_file_path = Path(__file__).parent.parent / "sql" / "calculate_doses.sql"
    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("Doses calculated")

    validation_result = validate_calculated_doses()
    if not validation_result["valid"]:
        logger.error("Dose calculation validation failed")
        logger.error(f"Validation results: {validation_result}")
    else:
        logger.info("Dose calculation validation completed successfully")

    return {
        "sql_result": result,
        "validation": validation_result,
        "status": "completed",
    }


if __name__ == "__main__":
    calculate_doses()
