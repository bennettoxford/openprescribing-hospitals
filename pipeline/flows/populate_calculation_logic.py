from prefect import flow, task, get_run_logger
from pipeline.utils.utils import execute_bigquery_query_from_sql_file, get_bigquery_client
from pipeline.bq_tables import CALCULATION_LOGIC_TABLE_SPEC, DOSE_TABLE_SPEC, DDD_QUANTITY_TABLE_SPEC
from pathlib import Path


@task
def validate_calculation_logic_consistency():
    """Validate calculation logic data for consistency and completeness"""
    logger = get_run_logger()
    client = get_bigquery_client()

    # Check for records with invalid logic types
    invalid_logic_types_query = f"""
    SELECT 
        logic_type,
        COUNT(*) AS count
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type NOT IN ('dose', 'ingredient', 'ddd')
    GROUP BY logic_type
    """
    results = client.query(invalid_logic_types_query).result()
    invalid_types = [dict(row) for row in results]

    if invalid_types:
        logger.error(f"Found {len(invalid_types)} invalid logic types")
        for record in invalid_types:
            logger.error(f"- Invalid logic type '{record['logic_type']}': {record['count']} records")

    # Check for ingredient logic without ingredient codes
    missing_ingredient_codes_query = f"""
    SELECT 
        vmp_code,
        logic_type,
        COUNT(*) AS count
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type = 'ingredient' AND ingredient_code IS NULL
    GROUP BY vmp_code, logic_type
    """
    results = client.query(missing_ingredient_codes_query).result()
    missing_ingredient_codes = [dict(row) for row in results]

    if missing_ingredient_codes:
        logger.error(f"Found {len(missing_ingredient_codes)} ingredient logic records without ingredient codes")
        for record in missing_ingredient_codes[:5]:
            logger.error(f"- VMP {record['vmp_code']}: {record['count']} ingredient logic records without ingredient code")

    # Check for dose logic with ingredient codes (dose should never have ingredient codes)
    dose_with_ingredient_codes_query = f"""
    SELECT 
        vmp_code,
        logic_type,
        ingredient_code,
        COUNT(*) AS count
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type = 'dose' AND ingredient_code IS NOT NULL
    GROUP BY vmp_code, logic_type, ingredient_code
    """
    results = client.query(dose_with_ingredient_codes_query).result()
    dose_with_ingredient_codes = [dict(row) for row in results]

    if dose_with_ingredient_codes:
        logger.error(f"Found {len(dose_with_ingredient_codes)} dose logic records with unexpected ingredient codes")
        for record in dose_with_ingredient_codes[:5]:
            logger.error(
                f"- VMP {record['vmp_code']} (dose): "
                f"{record['count']} records with ingredient code {record['ingredient_code']}"
            )

    # Check for DDD logic with ingredient codes that don't match ingredient-based calculations
    # DDD logic should only have ingredient codes when the calculation uses ingredient quantity
    invalid_ddd_ingredient_codes_query = f"""
    SELECT 
        vmp_code,
        logic_type,
        ingredient_code,
        logic,
        COUNT(*) AS count
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type = 'ddd' 
        AND ingredient_code IS NOT NULL
        AND logic NOT LIKE '%ingredient quantity%'
    GROUP BY vmp_code, logic_type, ingredient_code, logic
    """
    results = client.query(invalid_ddd_ingredient_codes_query).result()
    invalid_ddd_ingredient_codes = [dict(row) for row in results]

    if invalid_ddd_ingredient_codes:
        logger.error(f"Found {len(invalid_ddd_ingredient_codes)} DDD logic records with ingredient codes but non-ingredient-based calculations")
        for record in invalid_ddd_ingredient_codes[:5]:
            logger.error(
                f"- VMP {record['vmp_code']} (ddd): "
                f"{record['count']} records with ingredient code {record['ingredient_code']} "
                f"but logic '{record['logic']}' doesn't indicate ingredient-based calculation"
            )

    # Check for DDD logic using ingredient calculations but missing ingredient codes
    missing_ddd_ingredient_codes_query = f"""
    SELECT 
        vmp_code,
        logic_type,
        logic,
        COUNT(*) AS count
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type = 'ddd' 
        AND ingredient_code IS NULL
        AND logic LIKE '%ingredient quantity%'
    GROUP BY vmp_code, logic_type, logic
    """
    results = client.query(missing_ddd_ingredient_codes_query).result()
    missing_ddd_ingredient_codes = [dict(row) for row in results]

    if missing_ddd_ingredient_codes:
        logger.error(f"Found {len(missing_ddd_ingredient_codes)} DDD logic records using ingredient calculations but missing ingredient codes")
        for record in missing_ddd_ingredient_codes[:5]:
            logger.error(
                f"- VMP {record['vmp_code']} (ddd): "
                f"{record['count']} records with logic '{record['logic']}' "
                f"but missing ingredient code"
            )

    ddd_logic_summary_query = f"""
    SELECT 
        CASE 
            WHEN ingredient_code IS NOT NULL THEN 'ingredient-specific'
            ELSE 'vmp-level'
        END AS calculation_scope,
        COUNT(*) AS count,
        COUNT(DISTINCT vmp_code) AS unique_vmps
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type = 'ddd'
    GROUP BY calculation_scope
    ORDER BY calculation_scope
    """
    results = client.query(ddd_logic_summary_query).result()
    ddd_summary = [dict(row) for row in results]

    for record in ddd_summary:
        logger.info(
            f"DDD logic ({record['calculation_scope']}): "
            f"{record['count']} records across {record['unique_vmps']} VMPs"
        )

    return {
        "invalid_types": invalid_types,
        "missing_ingredient_codes": missing_ingredient_codes,
        "dose_with_ingredient_codes": dose_with_ingredient_codes,
        "invalid_ddd_ingredient_codes": invalid_ddd_ingredient_codes,
        "missing_ddd_ingredient_codes": missing_ddd_ingredient_codes,
        "valid": (
            len(invalid_types) == 0 and 
            len(missing_ingredient_codes) == 0 and 
            len(dose_with_ingredient_codes) == 0 and
            len(invalid_ddd_ingredient_codes) == 0 and
            len(missing_ddd_ingredient_codes) == 0
        )
    }

@task
def validate_logic_consistency_per_vmp():
    """Validate that each VMP has consistent logic within each logic type"""
    logger = get_run_logger()
    client = get_bigquery_client()

    # Check for VMPs with multiple different logic values for dose calculations
    dose_conflicts_query = f"""
    SELECT 
        vmp_code,
        COUNT(DISTINCT logic) AS logic_count,
        ARRAY_AGG(DISTINCT logic) AS different_logic_values
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type = 'dose'
    GROUP BY vmp_code
    HAVING COUNT(DISTINCT logic) > 1
    """
    results = client.query(dose_conflicts_query).result()
    dose_conflicts = [dict(row) for row in results]

    # Check for VMPs with multiple different logic values for DDD calculations
    ddd_conflicts_query = f"""
    SELECT 
        vmp_code,
        COUNT(DISTINCT logic) AS logic_count,
        ARRAY_AGG(DISTINCT logic) AS different_logic_values
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type = 'ddd'
    GROUP BY vmp_code
    HAVING COUNT(DISTINCT logic) > 1
    """
    results = client.query(ddd_conflicts_query).result()
    ddd_conflicts = [dict(row) for row in results]

    # Check for VMP-ingredient pairs with multiple different logic values
    ingredient_conflicts_query = f"""
    SELECT 
        vmp_code,
        ingredient_code,
        COUNT(DISTINCT logic) AS logic_count,
        ARRAY_AGG(DISTINCT logic) AS different_logic_values
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type = 'ingredient'
    GROUP BY vmp_code, ingredient_code
    HAVING COUNT(DISTINCT logic) > 1
    """
    results = client.query(ingredient_conflicts_query).result()
    ingredient_conflicts = [dict(row) for row in results]

    if dose_conflicts:
        logger.error(f"Found {len(dose_conflicts)} VMPs with conflicting dose calculation logic")
        for record in dose_conflicts[:5]:
            logger.error(f"- VMP {record['vmp_code']}: {record['logic_count']} different dose logic values")

    if ddd_conflicts:
        logger.error(f"Found {len(ddd_conflicts)} VMPs with conflicting DDD calculation logic")
        for record in ddd_conflicts[:5]:
            logger.error(f"- VMP {record['vmp_code']}: {record['logic_count']} different DDD logic values")

    if ingredient_conflicts:
        logger.error(f"Found {len(ingredient_conflicts)} VMP-ingredient pairs with conflicting logic")
        for record in ingredient_conflicts[:5]:
            logger.error(
                f"- VMP {record['vmp_code']} + Ingredient {record['ingredient_code']}: "
                f"{record['logic_count']} different logic values"
            )

    return {
        "dose_conflicts": dose_conflicts,
        "ddd_conflicts": ddd_conflicts,
        "ingredient_conflicts": ingredient_conflicts,
        "valid": len(dose_conflicts) == 0 and len(ddd_conflicts) == 0 and len(ingredient_conflicts) == 0
    }

@task
def validate_logic_coverage():
    """Validate that we have appropriate logic coverage for VMPs in other tables"""
    logger = get_run_logger()
    client = get_bigquery_client()

    # Check VMPs in dose table that don't have dose logic
    missing_dose_logic_query = f"""
    WITH dose_vmps AS (
        SELECT DISTINCT vmp_code
        FROM `{DOSE_TABLE_SPEC.full_table_id}`
    ),
    dose_logic_vmps AS (
        SELECT DISTINCT vmp_code
        FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
        WHERE logic_type = 'dose'
    )
    SELECT 
        d.vmp_code
    FROM dose_vmps d
    LEFT JOIN dose_logic_vmps dl ON d.vmp_code = dl.vmp_code
    WHERE dl.vmp_code IS NULL
    """
    results = client.query(missing_dose_logic_query).result()
    missing_dose_logic = [row["vmp_code"] for row in results]

    # Check VMPs in DDD table that don't have DDD logic
    missing_ddd_logic_query = f"""
    WITH ddd_vmps AS (
        SELECT DISTINCT vmp_code
        FROM `{DDD_QUANTITY_TABLE_SPEC.full_table_id}`
    ),
    ddd_logic_vmps AS (
        SELECT DISTINCT vmp_code
        FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
        WHERE logic_type = 'ddd'
    )
    SELECT 
        d.vmp_code
    FROM ddd_vmps d
    LEFT JOIN ddd_logic_vmps dl ON d.vmp_code = dl.vmp_code
    WHERE dl.vmp_code IS NULL
    """
    results = client.query(missing_ddd_logic_query).result()
    missing_ddd_logic = [row["vmp_code"] for row in results]

    if missing_dose_logic:
        logger.warning(f"Found {len(missing_dose_logic)} VMPs in dose table without dose calculation logic")
        for vmp_code in missing_dose_logic[:5]:
            logger.warning(f"- VMP {vmp_code}: No dose logic found")

    if missing_ddd_logic:
        logger.warning(f"Found {len(missing_ddd_logic)} VMPs in DDD table without DDD calculation logic")
        for vmp_code in missing_ddd_logic[:5]:
            logger.warning(f"- VMP {vmp_code}: No DDD logic found")

    return {
        "missing_dose_logic": missing_dose_logic,
        "missing_ddd_logic": missing_ddd_logic,
        "dose_coverage": len(missing_dose_logic) == 0,
        "ddd_coverage": len(missing_ddd_logic) == 0
    }


@task  
def get_calculation_logic_summary():
    """Get summary statistics for the calculation logic table"""
    logger = get_run_logger()
    client = get_bigquery_client()

    summary_query = f"""
    SELECT 
        logic_type,
        COUNT(*) AS total_records,
        COUNT(DISTINCT vmp_code) AS unique_vmps,
        COUNT(DISTINCT ingredient_code) AS unique_ingredients,
        COUNT(DISTINCT logic) AS unique_logic_patterns
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    GROUP BY logic_type
    ORDER BY logic_type
    """
    results = client.query(summary_query).result()
    summary = [dict(row) for row in results]

    for record in summary:
        logger.info(
            f"Logic type '{record['logic_type']}': "
            f"{record['total_records']} records, "
            f"{record['unique_vmps']} unique VMPs, "
            f"{record['unique_ingredients']} unique ingredients, "
            f"{record['unique_logic_patterns']} unique logic patterns"
        )

    return summary


@flow(name="Populate calculation logic")
def populate_calculation_logic():
    """
    Main flow to populate the calculation logic table from existing dose, ingredient, and DDD tables
    """
    logger = get_run_logger()
    logger.info("Populating calculation logic table")

    sql_file_path = Path(__file__).parent.parent / "sql" / "populate_calculation_logic.sql"

    sql_result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("Calculation logic table populated")

    summary = get_calculation_logic_summary()

    validation_results = {
        "consistency": validate_calculation_logic_consistency(),
        "coverage": validate_logic_coverage(),
        "logic_consistency_per_vmp": validate_logic_consistency_per_vmp()
    }

    validation_failed = False
    for key, result in validation_results.items():
        if not result.get("valid", True):
            if key == "consistency":
                validation_failed = True
                logger.error(f"Validation '{key}' failed")
            else:
                logger.warning(f"Validation '{key}' has warnings")

    if validation_failed:
        logger.error("Calculation logic validation failed")
    else:
        logger.info("Calculation logic validation completed successfully")

    return {
        "sql_result": sql_result,
        "summary": summary,
        "validation": validation_results,
        "validation_passed": not validation_failed
    }


if __name__ == "__main__":
    populate_calculation_logic() 