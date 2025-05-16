from prefect import flow, task, get_run_logger
from pathlib import Path
from pipeline.utils.utils import (
    get_bigquery_client,
    execute_bigquery_query_from_sql_file,
    validate_table_schema,
)
from pipeline.bq_tables import DMD_UOM_TABLE_SPEC


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

    if null_descriptions:
        logger.warning(
            f"Found {len(null_descriptions)} UOM codes with null descriptions:"
        )
        for row in null_descriptions[:5]:
            logger.warning(f"- {row.uom_code}")

    schema_valid = validate_table_schema(DMD_UOM_TABLE_SPEC)
    if not schema_valid:
        raise ValueError("Schema validation failed for UOM data")

    logger.info("UOM data validated")
    return {
        "schema_valid": schema_valid,
        "null_descriptions": len(null_descriptions)
    }


@flow(name="Import dm+d UOM")
def import_dmd_uom():
    logger = get_run_logger()
    logger.info("Importing dm+d UOM data")

    sql_file_path = Path(__file__).parent.parent / "sql" / "import_dmd_uom.sql"

    sql_result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    validation_result = validate_uom_data()

    logger.info("dm+d UOM data imported and validated")
    return {
        "sql_result": sql_result,
        "validation": validation_result,
    }


if __name__ == "__main__":
    import_dmd_uom()