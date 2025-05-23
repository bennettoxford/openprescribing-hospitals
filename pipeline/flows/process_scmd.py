from prefect import flow, task, get_run_logger
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
    get_bigquery_client,
    validate_table_schema,
)
from pathlib import Path
from pipeline.bq_tables import (
    ORGANISATION_TABLE_SPEC,
    UNITS_CONVERSION_TABLE_SPEC,
    SCMD_PROCESSED_TABLE_SPEC,
    VMP_UNIT_STANDARDISATION_TABLE_SPEC,
)


@task
def validate_processed_data():
    """Validate the processed SCMD data"""
    logger = get_run_logger()
    client = get_bigquery_client()

    schema_valid = validate_table_schema(SCMD_PROCESSED_TABLE_SPEC)
    if not schema_valid:
        raise ValueError("Schema validation failed for processed SCMD data")

    org_validation_query = f"""
    WITH scmd_orgs AS (
        SELECT DISTINCT ods_code
        FROM `{SCMD_PROCESSED_TABLE_SPEC.full_table_id}`
    )
    SELECT 
        s.ods_code,
        COUNT(*) as count
    FROM scmd_orgs s
    LEFT JOIN `{ORGANISATION_TABLE_SPEC.full_table_id}` o
        ON s.ods_code = o.ods_code
    WHERE o.ods_code IS NULL
    GROUP BY s.ods_code
    """

    units_validation_query = f"""
    WITH scmd_units AS (
        SELECT DISTINCT 
            CAST(CAST(normalised_uom_id AS STRING) AS STRING) as unit  -- Double cast to ensure string
        FROM `{SCMD_PROCESSED_TABLE_SPEC.full_table_id}`
    )
    SELECT 
        s.unit,
        COUNT(*) as count
    FROM scmd_units s
    LEFT JOIN `{UNITS_CONVERSION_TABLE_SPEC.full_table_id}` u
        ON CAST(s.unit AS STRING) = CAST(u.basis_id AS STRING)
    WHERE u.basis_id IS NULL
    GROUP BY s.unit
    """

    multiple_units_query = f"""
    WITH vmp_units AS (
        SELECT 
            vmp_code,
            vmp_name,
            COUNT(DISTINCT normalised_uom_id) as unit_count,  # Changed from uom_id to normalised_uom_id
            STRING_AGG(DISTINCT uom_id) as unit_ids,
            STRING_AGG(DISTINCT normalised_uom_id) as normalised_unit_ids,
            STRING_AGG(DISTINCT CONCAT(uom_name, ' (', uom_id, ' → ', normalised_uom_id, ')'), ', ') as unit_details
        FROM `{SCMD_PROCESSED_TABLE_SPEC.full_table_id}`
        GROUP BY vmp_code, vmp_name
        HAVING COUNT(DISTINCT normalised_uom_id) > 1  # Changed to check normalised units
    )
    SELECT 
        vu.vmp_code,
        vu.vmp_name,
        vu.unit_count,
        vu.unit_ids,
        vu.normalised_unit_ids,
        vu.unit_details
    FROM vmp_units vu
    LEFT JOIN `{VMP_UNIT_STANDARDISATION_TABLE_SPEC.full_table_id}` us
        ON vu.vmp_code = us.vmp_code
    WHERE us.vmp_code IS NULL
    ORDER BY vu.unit_count DESC, vu.vmp_code
    """

    missing_orgs = list(client.query(org_validation_query).result())
    missing_units = list(client.query(units_validation_query).result())
    vmps_multiple_units = list(client.query(multiple_units_query).result())

    if missing_orgs:
        logger.error(
            f"Found {len(missing_orgs)} organisations in SCMD not in reference table:"
        )
        for row in missing_orgs[:5]:
            logger.error(f"- {row.ods_code}: {row.count} occurrences")
        raise ValueError("Invalid organisations found in processed SCMD data")

    if missing_units:
        logger.error(
            f"Found {len(missing_units)} units in SCMD not in reference table:"
        )
        for row in missing_units[:5]:
            logger.error(f"- {row.unit}: {row.count} occurrences")
        raise ValueError("Invalid units found in processed SCMD data")

    if vmps_multiple_units:
        logger.warning(
            f"Found {len(vmps_multiple_units)} VMPs with multiple normalized units not in standardisation table:"
        )
        for row in vmps_multiple_units[:10]:
            logger.warning(
                f"- {row.vmp_code} ({row.vmp_name}): {row.unit_count} normalized units: {row.unit_details}"
            )

    logger.info("All validations passed")
    return {
        "schema_valid": schema_valid, 
        "orgs_valid": True, 
        "units_valid": True,
        "vmps_with_multiple_units": len(vmps_multiple_units)
    }


@flow(name="Process SCMD")
def process_scmd():
    logger = get_run_logger()
    logger.info("Processing SCMD")

    sql_file_path = Path(__file__).parent.parent / "sql" / "process_scmd.sql"

    sql_result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    validation_result = validate_processed_data()

    logger.info("SCMD processed and validated")

    return {"sql_result": sql_result, "validation": validation_result}


if __name__ == "__main__":
    process_scmd()
