from prefect import flow, task, get_run_logger
from pipeline.utils.utils import execute_bigquery_query_from_sql_file, get_bigquery_client
from pipeline.bq_tables import VMP_DDD_MAPPING_TABLE_SPEC, DMD_SUPP_TABLE_SPEC, WHO_DDD_TABLE_SPEC
from pathlib import Path


@task
def validate_ddd_mapping_consistency():
    """Validate VMP DDD mapping data for consistency"""
    logger = get_run_logger()
    client = get_bigquery_client()

    # Check for inconsistent DDD calculation status
    inconsistent_ddd_query = f"""
    SELECT 
        vmp_code,
        vmp_name,
        can_calculate_ddd,
        selected_ddd_value,
        selected_ddd_unit,
        selected_ddd_basis_unit,
        selected_ddd_route_code,
        ddd_calculation_logic
    FROM `{VMP_DDD_MAPPING_TABLE_SPEC.full_table_id}`
    WHERE 
        (can_calculate_ddd = TRUE AND (
            selected_ddd_value IS NULL OR
            selected_ddd_unit IS NULL OR
            selected_ddd_basis_unit IS NULL OR
            selected_ddd_route_code IS NULL
        ))
        OR
        (can_calculate_ddd = FALSE AND ddd_calculation_logic IS NULL)
    """
    results = client.query(inconsistent_ddd_query).result()
    inconsistent_records = [dict(row) for row in results]

    if inconsistent_records:
        logger.error(f"Found {len(inconsistent_records)} VMPs with inconsistent DDD calculation status")
        for record in inconsistent_records[:5]:
            if record['can_calculate_ddd']:
                logger.error(
                    f"- VMP {record['vmp_code']}: Marked as calculable but missing required DDD fields"
                )
            else:
                logger.error(
                    f"- VMP {record['vmp_code']}: Marked as non-calculable but missing explanation"
                )

    return {
        "inconsistent_records": inconsistent_records,
        "valid": len(inconsistent_records) == 0
    }


@task
def validate_atc_codes_exist():
    """Validate that VMPs marked as calculable have ATC codes"""
    logger = get_run_logger()
    client = get_bigquery_client()

    atc_query = f"""
    WITH calculable_vmps AS (
        SELECT vmp_code, vmp_name
        FROM `{VMP_DDD_MAPPING_TABLE_SPEC.full_table_id}`
        WHERE can_calculate_ddd = TRUE
    ),
    vmp_atcs AS (
        SELECT 
            vmp_code,
            COUNT(atc_code) AS atc_count
        FROM `{DMD_SUPP_TABLE_SPEC.full_table_id}`
        GROUP BY vmp_code
    )
    SELECT 
        v.vmp_code,
        v.vmp_name,
        COALESCE(a.atc_count, 0) AS atc_count
    FROM calculable_vmps v
    LEFT JOIN vmp_atcs a ON v.vmp_code = a.vmp_code
    WHERE COALESCE(a.atc_count, 0) = 0
    """
    
    results = client.query(atc_query).result()
    missing_atc_records = [dict(row) for row in results]
    
    if missing_atc_records:
        logger.error(f"Found {len(missing_atc_records)} VMPs marked as calculable but with no ATC codes")
        for record in missing_atc_records[:5]:
            logger.error(f"- VMP {record['vmp_code']} ({record['vmp_name']}): No ATC codes")
    
    return {
        "missing_atc_records": missing_atc_records,
        "valid": len(missing_atc_records) == 0
    }


@task
def validate_ddd_values_exist():
    """Validate that VMPs marked as calculable have WHO DDD values"""
    logger = get_run_logger()
    client = get_bigquery_client()

    ddd_query = f"""
    WITH calculable_vmps AS (
        SELECT 
            vmp_code, 
            vmp_name,
            selected_ddd_route_code
        FROM `{VMP_DDD_MAPPING_TABLE_SPEC.full_table_id}`
        WHERE can_calculate_ddd = TRUE
    ),
    vmp_atcs AS (
        SELECT 
            vmp_code,
            atc_code
        FROM `{DMD_SUPP_TABLE_SPEC.full_table_id}`
    ),
    vmp_ddds AS (
        SELECT 
            v.vmp_code,
            v.vmp_name,
            v.selected_ddd_route_code,
            COUNT(CASE WHEN who_ddd.ddd IS NOT NULL AND who_ddd.adm_code = v.selected_ddd_route_code THEN 1 END) AS matching_ddd_count
        FROM calculable_vmps v
        LEFT JOIN vmp_atcs a ON v.vmp_code = a.vmp_code
        LEFT JOIN `{WHO_DDD_TABLE_SPEC.full_table_id}` who_ddd ON a.atc_code = who_ddd.atc_code
        GROUP BY v.vmp_code, v.vmp_name, v.selected_ddd_route_code
    )
    SELECT 
        vmp_code,
        vmp_name,
        selected_ddd_route_code,
        matching_ddd_count
    FROM vmp_ddds
    WHERE matching_ddd_count = 0
    """
    
    results = client.query(ddd_query).result()
    missing_ddd_records = [dict(row) for row in results]
    
    if missing_ddd_records:
        logger.error(f"Found {len(missing_ddd_records)} VMPs marked as calculable but with no matching WHO DDD values")
        for record in missing_ddd_records[:5]:
            logger.error(
                f"- VMP {record['vmp_code']} ({record['vmp_name']}): "
                f"No DDD values for route {record['selected_ddd_route_code']}"
            )
    
    return {
        "missing_ddd_records": missing_ddd_records,
        "valid": len(missing_ddd_records) == 0
    }


@task
def validate_calculation_logic():
    """Validate that the DDD (Defined Daily Dose) calculation logic is consistent with VMP data.
    This validation checks for calculable VMPs only:

    1. For VMPs using SCMD unit calculation:
       - Verifies the selected DDD basis unit exists in the VMP's SCMD basis units

    2. For VMPs using ingredient quantity:
       - Verifies the VMP has exactly one ingredient
       - Verifies the ingredient's basis unit matches the selected DDD basis unit
    """
    logger = get_run_logger()
    client = get_bigquery_client()

    logic_query = f"""
    WITH ddd_mapping AS (
        SELECT 
            vmp_code,
            vmp_name,
            can_calculate_ddd,
            ddd_calculation_logic,
            selected_ddd_basis_unit,
            ARRAY_LENGTH(COALESCE(ingredients_info, [])) AS ingredient_count,
            CASE 
                WHEN ARRAY_LENGTH(COALESCE(ingredients_info, [])) = 1 
                THEN (
                    SELECT ingredient_basis_unit 
                    FROM UNNEST(ingredients_info) 
                    LIMIT 1
                )
                ELSE NULL
            END AS single_ingredient_basis_unit,
            -- Get SCMD basis units
            ARRAY(
                SELECT DISTINCT basis_name
                FROM UNNEST(uoms)
            ) AS scmd_basis_units
        FROM `{VMP_DDD_MAPPING_TABLE_SPEC.full_table_id}`
        WHERE can_calculate_ddd = TRUE
    )
    SELECT 
        vmp_code,
        vmp_name,
        ddd_calculation_logic,
        ingredient_count,
        selected_ddd_basis_unit,
        single_ingredient_basis_unit,
        scmd_basis_units,
        -- Check if DDD basis unit is in SCMD basis units
        (
            SELECT COUNT(1) 
            FROM UNNEST(scmd_basis_units) basis_unit
            WHERE basis_unit = selected_ddd_basis_unit
        ) > 0 AS scmd_basis_matches_ddd,
        CASE
            WHEN ddd_calculation_logic = 'Calculated using SCMD unit' 
                AND (
                    SELECT COUNT(1) 
                    FROM UNNEST(scmd_basis_units) basis_unit
                    WHERE basis_unit = selected_ddd_basis_unit
                ) > 0 THEN TRUE
            WHEN ddd_calculation_logic = 'Calculated using ingredient quantity' 
                AND ingredient_count = 1 
                AND single_ingredient_basis_unit = selected_ddd_basis_unit THEN TRUE
            ELSE FALSE
        END AS logic_is_valid
    FROM ddd_mapping
    WHERE CASE
        WHEN ddd_calculation_logic = 'Calculated using SCMD unit' 
            AND (
                SELECT COUNT(1) 
                FROM UNNEST(scmd_basis_units) basis_unit
                WHERE basis_unit = selected_ddd_basis_unit
            ) > 0 THEN TRUE
        WHEN ddd_calculation_logic = 'Calculated using ingredient quantity' 
            AND ingredient_count = 1 
            AND single_ingredient_basis_unit = selected_ddd_basis_unit THEN TRUE
        ELSE FALSE
    END = FALSE
    """
    
    results = client.query(logic_query).result()
    invalid_logic_records = [dict(row) for row in results]
    
    if invalid_logic_records:
        logger.error(f"Found {len(invalid_logic_records)} VMPs with inconsistent calculation logic")
        for record in invalid_logic_records[:5]:
            if record['ddd_calculation_logic'] == 'Calculated using SCMD unit':
                if not record.get('scmd_basis_matches_ddd', False):
                    logger.error(
                        f"- VMP {record['vmp_code']} ({record['vmp_name']}): "
                        f"Using SCMD unit but DDD basis unit ({record['selected_ddd_basis_unit']}) "
                        f"not in SCMD basis units ({', '.join(record['scmd_basis_units'])})"
                    )
            elif record['ddd_calculation_logic'] == 'Calculated using ingredient quantity':
                if record['ingredient_count'] != 1:
                    logger.error(
                        f"- VMP {record['vmp_code']} ({record['vmp_name']}): "
                        f"Using ingredient fallback but has {record['ingredient_count']} ingredients"
                    )
                elif record['single_ingredient_basis_unit'] != record['selected_ddd_basis_unit']:
                    logger.error(
                        f"- VMP {record['vmp_code']} ({record['vmp_name']}): "
                        f"Ingredient basis unit ({record['single_ingredient_basis_unit']}) "
                        f"doesn't match DDD basis unit ({record['selected_ddd_basis_unit']})"
                    )
            else:
                logger.error(
                    f"- VMP {record['vmp_code']} ({record['vmp_name']}): "
                    f"Invalid calculation logic: {record['ddd_calculation_logic']}"
                )
    
    return {
        "invalid_logic_records": invalid_logic_records,
        "valid": len(invalid_logic_records) == 0
    }


@flow(name="Create VMP DDD mapping")
def create_vmp_ddd_mapping():
    logger = get_run_logger()
    logger.info("Creating VMP DDD mapping")

    sql_file_path = Path(__file__).parent.parent / "sql" / "create_vmp_ddd_mapping.sql"

    sql_result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD mapping calculated")

    validation_results = {
        "consistency": validate_ddd_mapping_consistency(),
        "atc_codes": validate_atc_codes_exist(),
        "ddd_values": validate_ddd_values_exist(),
        "calculation_logic": validate_calculation_logic()
    }
    validation_failed = False
    for key, result in validation_results.items():
        if not result.get("valid", False):
            validation_failed = True
            logger.error(f"Validation '{key}' failed")
    
    if validation_failed:
        logger.error("DDD mapping validation failed")
    else:
        logger.info("DDD mapping validation completed successfully")

    return {
        "sql_result": sql_result,
        "validation": validation_results,
        "validation_passed": not validation_failed
    }


if __name__ == "__main__":
    create_vmp_ddd_mapping()
