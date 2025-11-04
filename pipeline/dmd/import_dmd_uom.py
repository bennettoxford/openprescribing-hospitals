from prefect import flow, task, get_run_logger
from pathlib import Path
from pipeline.utils.utils import (
    get_bigquery_client,
    execute_bigquery_query_from_sql_file,
    validate_table_schema,
)
from pipeline.setup.bq_tables import DMD_UOM_TABLE_SPEC


@task
def validate_uom_data():
    """Validate the UOM data"""
    logger = get_run_logger()
    client = get_bigquery_client()

    # Check for null descriptions
    null_desc_query = f"""
    SELECT uom_code, description
    FROM `{DMD_UOM_TABLE_SPEC.full_table_id}`
    WHERE description IS NULL
    """

    results = client.query(null_desc_query).result()
    null_descriptions = list(results)

    # Validate code uniqueness
    duplicate_codes_query = f"""
    WITH all_codes AS (
        SELECT uom_code as code FROM `{DMD_UOM_TABLE_SPEC.full_table_id}`
        UNION ALL
        SELECT uom_code_prev as code 
        FROM `{DMD_UOM_TABLE_SPEC.full_table_id}`
        WHERE uom_code_prev IS NOT NULL
    )
    SELECT code, COUNT(*) as occurrences
    FROM all_codes
    WHERE code IS NOT NULL
    GROUP BY code
    HAVING COUNT(*) > 1
    """
    results = client.query(duplicate_codes_query).result()
    duplicate_codes = [dict(row) for row in results]


    if null_descriptions:
        logger.warning(
            f"Found {len(null_descriptions)} UOM codes with null descriptions:"
        )
        for row in null_descriptions[:5]:
            logger.warning(f"- {row.uom_code}")

    if duplicate_codes:
        logger.error(f"Found {len(duplicate_codes)} codes that appear multiple times")
        for code in duplicate_codes[:5]:
            logger.error(f"- Code {code['code']} appears {code['occurrences']} times")


    schema_valid = validate_table_schema(DMD_UOM_TABLE_SPEC)
    if not schema_valid:
        raise ValueError("Schema validation failed for UOM data")

    logger.info("UOM data validated")
    return {
        "schema_valid": schema_valid,
        "null_descriptions": len(null_descriptions),
        "duplicate_codes": duplicate_codes,
        "valid": schema_valid and not duplicate_codes
    }


@flow(name="Import dm+d UOM")
def import_dmd_uom():
    logger = get_run_logger()
    logger.info("Importing dm+d UOM data")

    sql_file_path = Path(__file__).parent / "import_dmd_uom.sql"

    sql_result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    validation_result = validate_uom_data()

    if not validation_result["valid"]:
        logger.error("UOM data validation failed")
        logger.error(f"Validation results: {validation_result}")
    else:
        logger.info("dm+d UOM data imported and validated successfully")


if __name__ == "__main__":
    import_dmd_uom()