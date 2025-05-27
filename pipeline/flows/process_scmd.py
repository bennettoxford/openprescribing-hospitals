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
    DMD_TABLE_SPEC,
)

@task
def validate_scmd_data():
    """Validate all aspects of SCMD data including schema, references, and temporal consistency"""
    logger = get_run_logger()
    client = get_bigquery_client()

    # Schema validation
    schema_valid = validate_table_schema(SCMD_PROCESSED_TABLE_SPEC)
    if not schema_valid:
        raise ValueError("Schema validation failed for processed SCMD data")

    validation_queries = {
        "org_validation": f"""
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
        """,
        "units_validation": f"""
            WITH scmd_units AS (
                SELECT DISTINCT 
                    CAST(CAST(normalised_uom_id AS STRING) AS STRING) as unit
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
        """,
        "vmp_mappings": f"""
            WITH processed_vmps AS (
                SELECT DISTINCT vmp_code, vmp_name
                FROM `{SCMD_PROCESSED_TABLE_SPEC.full_table_id}`
            ),
            dmd_vmps AS (
                SELECT DISTINCT vmp_code
                FROM `{DMD_TABLE_SPEC.full_table_id}`
            )
            SELECT 
                p.vmp_code,
                p.vmp_name,
                'Missing in DMD' as status,
                COUNT(*) as count_in_processed
            FROM processed_vmps p
            LEFT JOIN dmd_vmps d ON p.vmp_code = d.vmp_code
            WHERE d.vmp_code IS NULL
            GROUP BY 1, 2, 3
        """,
        "multiple_units": f"""
            WITH vmp_units AS (
                SELECT 
                    vmp_code,
                    vmp_name,
                    COUNT(DISTINCT normalised_uom_id) as unit_count,
                    STRING_AGG(DISTINCT uom_id) as unit_ids,
                    STRING_AGG(DISTINCT normalised_uom_id) as normalised_unit_ids,
                    STRING_AGG(DISTINCT CONCAT(uom_name, ' (', uom_id, ' → ', normalised_uom_id, ')'), ', ') as unit_details
                FROM `{SCMD_PROCESSED_TABLE_SPEC.full_table_id}`
                GROUP BY vmp_code, vmp_name
                HAVING COUNT(DISTINCT normalised_uom_id) > 1
            )
            SELECT *
            FROM vmp_units vu
            LEFT JOIN `{VMP_UNIT_STANDARDISATION_TABLE_SPEC.full_table_id}` us
                ON vu.vmp_code = us.vmp_code
            WHERE us.vmp_code IS NULL
            ORDER BY vu.unit_count DESC, vu.vmp_code
        """,
        "temporal_consistency": f"""
            WITH vmp_units AS (
                SELECT 
                    vmp_code,
                    vmp_name,
                    year_month,
                    normalised_uom_id,
                    LAG(normalised_uom_id) OVER (
                        PARTITION BY vmp_code 
                        ORDER BY year_month
                    ) as prev_uom
                FROM `{SCMD_PROCESSED_TABLE_SPEC.full_table_id}`
            )
            SELECT DISTINCT
                vmp_code,
                vmp_name,
                year_month,
                normalised_uom_id as current_uom,
                prev_uom as previous_uom
            FROM vmp_units
            WHERE normalised_uom_id != prev_uom
            AND prev_uom IS NOT NULL
            ORDER BY year_month, vmp_code
        """
    }

    validation_results = {}
    for check_name, query in validation_queries.items():
        results = list(client.query(query).result())
        validation_results[check_name] = results

    if validation_results["org_validation"]:
        logger.error(f"Found {len(validation_results['org_validation'])} invalid organisations")
        for row in validation_results["org_validation"][:5]:
            logger.error(f"- {row.ods_code}: {row.count} occurrences")
        raise ValueError("Invalid organisations found")

    if validation_results["units_validation"]:
        logger.error(f"Found {len(validation_results['units_validation'])} invalid units")
        for row in validation_results["units_validation"][:5]:
            logger.error(f"- {row.unit}: {row.count} occurrences")
        raise ValueError("Invalid units found")

    if validation_results["vmp_mappings"]:
        logger.error(f"Found {len(validation_results['vmp_mappings'])} invalid VMP mappings")
        for row in validation_results["vmp_mappings"][:5]:
            logger.error(f"- VMP {row.vmp_code} ({row.vmp_name}): {row.status}")

        prev_code_query = f"""
            SELECT vmp_code, vmp_code_prev
            FROM `{DMD_TABLE_SPEC.full_table_id}`
            WHERE vmp_code_prev IN ({','.join([f"'{m.vmp_code}'" for m in validation_results['vmp_mappings']])})
        """
        prev_mappings = list(client.query(prev_code_query).result())
        if prev_mappings:
            logger.error("\nSome invalid codes found as previous codes:")
            for prev in prev_mappings:
                logger.error(f"- Previous code {prev.vmp_code_prev} should be mapped to {prev.vmp_code}")
        raise ValueError("Invalid VMP mappings found")

    if validation_results["multiple_units"]:
        logger.warning(f"Found {len(validation_results['multiple_units'])} VMPs with multiple units")
        for row in validation_results["multiple_units"][:5]:
            logger.warning(f"- {row.vmp_code} ({row.vmp_name}): {row.unit_details}")

    if validation_results["temporal_consistency"]:
        logger.warning(f"Found {len(validation_results['temporal_consistency'])} temporal unit changes")
        for row in validation_results["temporal_consistency"][:5]:
            logger.warning(
                f"- {row.vmp_code} ({row.vmp_name}) changed from {row.previous_uom} "
                f"to {row.current_uom} in {row.year_month}"
            )

    return {
        "schema_valid": schema_valid,
        "orgs_valid": len(validation_results["org_validation"]) == 0,
        "units_valid": len(validation_results["units_validation"]) == 0,
        "vmp_mappings_valid": len(validation_results["vmp_mappings"]) == 0,
        "vmps_with_multiple_units": len(validation_results["multiple_units"]),
        "temporal_unit_changes": len(validation_results["temporal_consistency"])
    }

@flow(name="Process SCMD")
def process_scmd():
    logger = get_run_logger()
    logger.info("Processing SCMD")

    sql_file_path = Path(__file__).parent.parent / "sql" / "process_scmd.sql"
    sql_result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    validation_results = validate_scmd_data()

    logger.info("SCMD processed and validated")
    return {
        "sql_result": sql_result,
        "validations": validation_results
    }

if __name__ == "__main__":
    process_scmd()
