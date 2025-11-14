from prefect import flow, task, get_run_logger
from pipeline.utils.utils import (
    get_bigquery_client,
    execute_bigquery_query_from_sql_file,
    validate_table_schema,
)
from pipeline.setup.bq_tables import INGREDIENT_QUANTITY_TABLE_SPEC
from pathlib import Path


@task
def validate_ingredients_exist():
    """Validate that ingredient quantities only exist with valid ingredients"""
    logger = get_run_logger()
    logger.info("Validating ingredient existence")

    client = get_bigquery_client()
    query = f"""
    WITH unnested_data AS (
        SELECT 
            vmp_code,
            vmp_name,
            ing.*
        FROM `{INGREDIENT_QUANTITY_TABLE_SPEC.full_table_id}`,
        UNNEST(ingredients) as ing
    )
    SELECT
        vmp_code,
        vmp_name,
        ingredient_code,
        ingredient_name,
        ingredient_quantity
    FROM unnested_data
    WHERE ingredient_quantity IS NOT NULL 
        AND ingredient_code IS NULL
    """
    
    results = client.query(query).result()
    errors = list(results)
    
    if errors:
        logger.error(f"Found {len(errors)} cases of ingredient quantities without valid ingredients:")
        for error in errors:
            logger.error(f"VMP: {error.vmp_code} ({error.vmp_name})")
        raise ValueError("Ingredient existence validation failed")
    
    logger.info("All ingredient quantities have valid ingredients")
    return {"ingredient_existence_validation": "passed"}


@task
def validate_strength_info():
    """Validate that ingredient quantities only exist with complete strength information"""
    logger = get_run_logger()
    logger.info("Validating strength information")

    client = get_bigquery_client()
    query = f"""
    WITH unnested_data AS (
        SELECT 
            vmp_code,
            vmp_name,
            ing.*
        FROM `{INGREDIENT_QUANTITY_TABLE_SPEC.full_table_id}`,
        UNNEST(ingredients) as ing
    )
    SELECT
        vmp_code,
        vmp_name,
        ingredient_code,
        ingredient_name,
        ingredient_quantity,
        strength_numerator_value,
        strength_numerator_unit
    FROM unnested_data
    WHERE ingredient_quantity IS NOT NULL 
        AND (strength_numerator_value IS NULL OR strength_numerator_unit IS NULL)
    """
    
    results = client.query(query).result()
    errors = list(results)
    
    if errors:
        logger.error(f"Found {len(errors)} cases of ingredient quantities without complete strength information:")
        for error in errors:
            logger.error(f"VMP: {error.vmp_code} ({error.vmp_name}), Ingredient: {error.ingredient_code}")
        raise ValueError("Strength information validation failed")
    
    logger.info("All ingredient quantities have complete strength information")
    return {"strength_info_validation": "passed"}


@task
def validate_direct_multiplication():
    """Validate calculations where no denominator is present"""
    logger = get_run_logger()
    logger.info("Validating direct multiplication calculations")

    client = get_bigquery_client()
    query = f"""
    WITH unnested_data AS (
        SELECT 
            vmp_code,
            vmp_name,
            converted_quantity as scmd_quantity,
            ing.*
        FROM `{INGREDIENT_QUANTITY_TABLE_SPEC.full_table_id}`,
        UNNEST(ingredients) as ing
    )
    SELECT
        vmp_code,
        vmp_name,
        ingredient_code,
        ingredient_name,
        ingredient_quantity,
        ingredient_quantity_basis,
        scmd_quantity,
        strength_numerator_value,
        ABS(ingredient_quantity - (scmd_quantity * strength_numerator_value)) as difference
    FROM unnested_data
    WHERE ingredient_quantity IS NOT NULL 
        AND strength_denominator_value IS NULL
        AND ABS(ingredient_quantity - (scmd_quantity * strength_numerator_value)) > 0.0001
    """
    
    results = client.query(query).result()
    errors = list(results)
    
    if errors:
        logger.error(f"Found {len(errors)} cases of direct multiplication mismatches:")
        for error in errors:
            logger.error(
                f"VMP: {error.vmp_code} ({error.vmp_name}), "
                f"Ingredient: {error.ingredient_code}, "
                f"Difference: {error.difference}"
            )
        raise ValueError("Direct multiplication validation failed")
    
    logger.info("All direct multiplication calculations are correct")
    return {"direct_multiplication_validation": "passed"}


@task
def validate_denominator_calculations():
    """Validate calculations where a denominator is present"""
    logger = get_run_logger()
    logger.info("Validating denominator-based calculations")

    client = get_bigquery_client()
    query = f"""
    WITH unnested_data AS (
        SELECT 
            vmp_code,
            vmp_name,
            quantity_basis,
            ing.*
        FROM `{INGREDIENT_QUANTITY_TABLE_SPEC.full_table_id}`,
        UNNEST(ingredients) as ing
    )
    SELECT
        vmp_code,
        vmp_name,
        ingredient_code,
        ingredient_name,
        ingredient_quantity,
        calculation_logic,
        quantity_basis,
        denominator_basis_unit,
        strength_denominator_value
    FROM unnested_data
    WHERE ingredient_quantity IS NOT NULL 
        AND strength_denominator_value IS NOT NULL
        AND quantity_basis != denominator_basis_unit
    """
    
    results = client.query(query).result()
    errors = list(results)
    
    if errors:
        logger.error(f"Found {len(errors)} cases where denominator basis units don't match quantity basis units:")
        for error in errors:
            logger.error(
                f"VMP: {error.vmp_code} ({error.vmp_name}), "
                f"Ingredient: {error.ingredient_code}, "
                f"Quantity basis: {error.quantity_basis}, "
                f"Denominator basis: {error.denominator_basis_unit}"
            )
        raise ValueError("Denominator calculation validation failed")
    
    logger.info("All denominator-based calculations use matching basis units")
    return {"denominator_calculations_validation": "passed"}


@task
def validate_calculation_logic_consistency():
    """Validate that ingredient calculations match the pre-calculated logic"""
    logger = get_run_logger()
    logger.info("Validating calculation logic consistency")

    client = get_bigquery_client()
    query = f"""
    WITH unnested_data AS (
        SELECT 
            vmp_code,
            vmp_name,
            ing.*
        FROM `{INGREDIENT_QUANTITY_TABLE_SPEC.full_table_id}`,
        UNNEST(ingredients) as ing
    )
    SELECT
        vmp_code,
        vmp_name,
        ingredient_code,
        ingredient_name,
        calculation_logic,
        ingredient_quantity,
        CASE
            WHEN ingredient_quantity IS NOT NULL AND calculation_logic LIKE 'Not calculated:%'
                THEN 'Ingredient calculated despite logic indicating it should not be'
            WHEN ingredient_quantity IS NULL AND calculation_logic NOT LIKE 'Not calculated:%'
                THEN 'Ingredient not calculated despite logic indicating it should be'
        END as issue
    FROM unnested_data
    WHERE 
        (ingredient_quantity IS NOT NULL AND calculation_logic LIKE 'Not calculated:%')
        OR (ingredient_quantity IS NULL AND calculation_logic NOT LIKE 'Not calculated:%')
    """
    
    results = client.query(query).result()
    errors = list(results)
    
    if errors:
        logger.error(f"Found {len(errors)} calculation logic consistency issues:")
        for error in errors[:5]:
            logger.error(
                f"VMP: {error.vmp_code} ({error.vmp_name}), "
                f"Ingredient: {error.ingredient_code}, "
                f"{error.issue} (Logic: {error.calculation_logic})"
            )
    else:
        logger.info("All ingredient calculations match the pre-calculated logic")
    
    return {"calculation_logic_consistency": "passed" if not errors else "failed", "error_count": len(errors)}


@task
def validate_ingredient_quantity():
    """Validate the ingredient quantity data schema"""
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
        Path(__file__).parent / "calculate_ingredient_quantity.sql"
    )

    result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("Ingredient quantities calculated")

    validate_ingredient_quantity()
    validate_calculation_logic_consistency()
    validate_ingredients_exist()
    validate_strength_info()
    validate_direct_multiplication()
    validate_denominator_calculations()




if __name__ == "__main__":
    calculate_ingredient_quantity()
