from prefect import flow, task, get_run_logger
from pathlib import Path
from pipeline.utils.utils import (
    get_bigquery_client,
    execute_bigquery_query_from_sql_file,
    validate_table_schema,
)
from pipeline.bq_tables import DMD_TABLE_SPEC, ADM_ROUTE_MAPPING_TABLE_SPEC


@task
def validate_routes():
    """Validate that all routes in DMD exist in route mapping table"""
    logger = get_run_logger()
    client = get_bigquery_client()

    query = f"""
    WITH unique_routes AS (
        SELECT DISTINCT ontformroute_descr
        FROM `{DMD_TABLE_SPEC.full_table_id}`,
        UNNEST(ontformroutes)
    )
    SELECT ontformroute_descr
    FROM unique_routes
    WHERE ontformroute_descr NOT IN (
        SELECT dmd_ontformroute
        FROM `{ADM_ROUTE_MAPPING_TABLE_SPEC.full_table_id}`
    )
    """

    results = client.query(query).result()
    missing_routes = [row.ontformroute_descr for row in results]

    if missing_routes:
        logger.error(
            f"Found routes in DMD that are not in route mapping table: {missing_routes}"
        )
        return {"valid": False, "missing_routes": missing_routes}

    logger.info("All DMD routes are present in route mapping table")
    return {"valid": True}

@task
def validate_vtm_consistency():
    """Validate VTM relationships are consistent"""
    logger = get_run_logger()
    client = get_bigquery_client()

    query = f"""
    SELECT vmp_code, vmp_name
    FROM `{DMD_TABLE_SPEC.full_table_id}`
    WHERE (vtm IS NOT NULL AND vtm_name IS NULL)
    OR (vtm IS NULL AND vtm_name IS NOT NULL)
    """

    results = client.query(query).result()
    inconsistent_vtms = [{"vmp_code": row.vmp_code, "vmp_name": row.vmp_name} for row in results]

    if inconsistent_vtms:
        logger.warning(f"Found inconsistent VTM relationships: {inconsistent_vtms}")
        return {"valid": False, "inconsistent_vtms": inconsistent_vtms}

    logger.info("All VTM relationships are consistent")
    return {"valid": True}

@task
def validate_ingredient_units():
    """Validate ingredient strength units are present when values exist"""
    logger = get_run_logger()
    client = get_bigquery_client()

    query = f"""
    SELECT vmp_code, vmp_name, ing.ing_name
    FROM `{DMD_TABLE_SPEC.full_table_id}`,
    UNNEST(ingredients) as ing
    WHERE (ing.strnt_nmrtr_val IS NOT NULL AND ing.strnt_nmrtr_uom_name IS NULL)
    OR (ing.strnt_dnmtr_val IS NOT NULL AND ing.strnt_dnmtr_uom_name IS NULL)
    """

    results = client.query(query).result()
    missing_units = [dict(row) for row in results]

    if missing_units:
        logger.warning(f"Found ingredients with missing strength units: {missing_units}")
        return {"valid": False, "missing_units": missing_units}

    logger.info("All ingredient strength values have corresponding units")
    return {"valid": True}

@flow(name="Import dm+d")
def import_dmd():
    """Import and validate dm+d data"""
    logger = get_run_logger()
    logger.info("Importing dm+d")

    sql_file_path = Path(__file__).parent.parent / "sql" / "import_dmd.sql"
    sql_result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("dm+d imported")

    validations = {
        "routes": validate_routes(),
        "schema": validate_table_schema(DMD_TABLE_SPEC),
        "vtm": validate_vtm_consistency(),
        "ingredient_units": validate_ingredient_units()
    }

    failed_validations = {
        name: result for name, result in validations.items() 
        if isinstance(result, dict) and result.get("valid") is False
    }

    if failed_validations:
        logger.error(f"Validation failures: {failed_validations}")
    else:
        logger.info("All validations passed successfully")

    return {
        "sql_result": sql_result,
        "validations": validations,
    }


if __name__ == "__main__":
    import_dmd()
