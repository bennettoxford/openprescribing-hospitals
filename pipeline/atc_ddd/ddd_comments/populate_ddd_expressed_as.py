from google.cloud import bigquery
from prefect import task, flow, get_run_logger
from pipeline.utils.utils import execute_bigquery_query_from_sql_file, validate_table_schema, get_bigquery_client
from pipeline.setup.bq_tables import VMP_EXPRESSED_AS_TABLE_SPEC
from pipeline.setup.config import (
    PROJECT_ID,
    DATASET_ID,
    VMP_TABLE_ID,
    DMD_TABLE_ID,
    DMD_SUPP_TABLE_ID,
    ADM_ROUTE_MAPPING_TABLE_ID,
    WHO_DDD_TABLE_ID
)
from pathlib import Path
import pandas as pd


@task
def validate_expressed_as_vmps(df: pd.DataFrame) -> pd.DataFrame:
    """Validate that VMP IDs exist in VMP table and names match"""
    logger = get_run_logger()
    client = get_bigquery_client()

    vmp_query = f"""
    SELECT vmp_code, vmp_name
    FROM `{PROJECT_ID}.{DATASET_ID}.{VMP_TABLE_ID}`
    """

    results = client.query(vmp_query).result()
    valid_vmps = {str(row.vmp_code): row.vmp_name for row in results}

    invalid_vmp_ids = df[~df["vmp_id"].astype(str).isin(valid_vmps.keys())]
    if not invalid_vmp_ids.empty:
        raise ValueError(f"Invalid VMP IDs found: {invalid_vmp_ids['vmp_id'].unique().tolist()}")

    df_temp = df.copy()
    df_temp["vmp_id_str"] = df_temp["vmp_id"].astype(str)
    df_temp["expected_vmp_name"] = df_temp["vmp_id_str"].map(valid_vmps)
    mismatched = df_temp[df_temp["vmp_name"] != df_temp["expected_vmp_name"]]

    if not mismatched.empty:
        raise ValueError(f"VMP name mismatches found for VMP IDs: {mismatched['vmp_id'].tolist()}")

    logger.info("All VMPs validated successfully")


@task
def validate_expressed_as_units(df: pd.DataFrame) -> pd.DataFrame:
    """Validate that unit IDs exist and unit names match"""
    logger = get_run_logger()
    client = get_bigquery_client()

    units_query = f"""
    SELECT unit_id, unit
    FROM `{PROJECT_ID}.{DATASET_ID}.unit_conversion`
    WHERE unit_id IS NOT NULL
    """

    results = client.query(units_query).result()
    valid_units = {str(row.unit_id): row.unit for row in results}

    invalid_units = df[~df["expressed_as_strnt_nmrtr_uom"].astype(str).isin(valid_units.keys())]
    if not invalid_units.empty:
        raise ValueError(f"Invalid unit IDs found: {invalid_units['expressed_as_strnt_nmrtr_uom'].unique()}")

    df_temp = df.copy()
    df_temp["unit_id_str"] = df_temp["expressed_as_strnt_nmrtr_uom"].astype(str)
    df_temp["expected_unit_name"] = df_temp["unit_id_str"].map(valid_units)
    mismatched = df_temp[df_temp["expressed_as_strnt_nmrtr_uom_name"] != df_temp["expected_unit_name"]]

    if not mismatched.empty:
        raise ValueError(f"Unit name mismatches found for VMPs: {mismatched['vmp_id'].tolist()}")

    logger.info("All units validated successfully")



@task
def validate_expressed_as_ingredients(df: pd.DataFrame):
    """Validate that ingredient codes exist in VMP ingredients and names match"""
    logger = get_run_logger()
    client = get_bigquery_client()

    vmp_codes = df["vmp_id"].astype(str).tolist()

    vmp_query = f"""
    SELECT
        vmp_code,
        ARRAY_AGG(STRUCT(ing.ingredient_code, ing.ingredient_name)) AS ingredients
    FROM `{PROJECT_ID}.{DATASET_ID}.{VMP_TABLE_ID}` vmp,
    UNNEST(vmp.ingredients) AS ing
    WHERE vmp_code IN UNNEST(@vmp_codes)
    GROUP BY vmp_code
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("vmp_codes", "STRING", vmp_codes)
        ]
    )

    results = client.query(vmp_query, job_config=job_config).result()
    vmp_ingredients = {}
    for row in results:
        vmp_ingredients[str(row.vmp_code)] = {
            (str(ing['ingredient_code']), ing['ingredient_name'])
            for ing in row.ingredients
        }

    invalid_vmps = []
    mismatched_ingredients = []

    for _, row in df.iterrows():
        vmp_id = str(row["vmp_id"])
        ingredient_code = str(row["ingredient_code"])
        ingredient_name = row["ingredient_name"]

        if vmp_id not in vmp_ingredients:
            invalid_vmps.append(vmp_id)
            continue

        ingredient_set = vmp_ingredients[vmp_id]
        if (ingredient_code, ingredient_name) not in ingredient_set:
            mismatched_ingredients.append({
                "vmp_id": vmp_id,
                "ingredient_code": ingredient_code,
                "ingredient_name": ingredient_name
            })

    if invalid_vmps:
        raise ValueError(f"VMP IDs with no ingredients found (or VMPs not in table): {list(set(invalid_vmps))}")

    if mismatched_ingredients:
        raise ValueError(f"Ingredient code/name mismatches found: {mismatched_ingredients}")

    logger.info("All ingredients validated successfully")


@task
def validate_all_expressed_as_vmps_covered(df: pd.DataFrame) -> pd.DataFrame:
    """Validate that all VMPs with 'expressed as' DDD comments are covered in the populated table"""
    logger = get_run_logger()
    client = get_bigquery_client()

    # Get all VMPs that have the "expressed as" DDD comments we're looking for
    expected_vmps_query = f"""
    SELECT DISTINCT
        dmd.vmp_code,
        dmd.vmp_name,
        ddd.comment AS ddd_comment
    FROM `{PROJECT_ID}.{DATASET_ID}.{DMD_TABLE_ID}` dmd
    INNER JOIN `{PROJECT_ID}.{DATASET_ID}.{VMP_TABLE_ID}` vmp_table
        ON dmd.vmp_code = vmp_table.vmp_code
    INNER JOIN `{PROJECT_ID}.{DATASET_ID}.{DMD_SUPP_TABLE_ID}` dmd_supp
        ON dmd.vmp_code = dmd_supp.vmp_code
    LEFT JOIN UNNEST(dmd.ontformroutes) AS route
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{ADM_ROUTE_MAPPING_TABLE_ID}` routelookup
        ON route.ontformroute_descr = routelookup.dmd_ontformroute
    INNER JOIN `{PROJECT_ID}.{DATASET_ID}.{WHO_DDD_TABLE_ID}` ddd
        ON dmd_supp.atc_code = ddd.atc_code
        AND routelookup.who_route = ddd.adm_code
    WHERE dmd_supp.atc_code IS NOT NULL
        AND ddd.comment IS NOT NULL
        AND TRIM(ddd.comment) != ''
        AND ddd.comment NOT LIKE 'Refers to%'
        AND LOWER(TRIM(ddd.comment)) IN (
            'expressed as folinic acid', 'expressed as lanthanum',
            'expressed as levofolinic acid', 'expressed as benzylpenicillin',
            'expressed as aclidinium, delivered dose', 'expressed as glycopyrronium, delivered dose',
            'expressed as tiotropium, delivered dose', 'expressed as umeclidinium, delivered dose',
            'anti xa', 'fe', 'fe2+'
        )
    """

    results = client.query(expected_vmps_query).result()
    expected_vmps = {str(row.vmp_code): {"name": row.vmp_name, "comment": row.ddd_comment} for row in results}

    # Get VMPs that were actually populated
    populated_vmps = set(df["vmp_id"].astype(str).tolist())

    # Find missing VMPs
    expected_vmp_codes = set(expected_vmps.keys())
    missing_vmps = expected_vmp_codes - populated_vmps

    if missing_vmps:
        missing_details = [
            {
                "vmp_code": vmp_code,
                "vmp_name": expected_vmps[vmp_code]["name"],
                "ddd_comment": expected_vmps[vmp_code]["comment"]
            }
            for vmp_code in missing_vmps
        ]
        raise ValueError(f"VMPs with 'expressed as' DDD comments not covered in populated table: {missing_details}")

    logger.info(f"All {len(expected_vmps)} VMPs with 'expressed as' DDD comments are covered in the populated table")


@task
def get_populated_data() -> pd.DataFrame:
    """Get the data that was just populated in BigQuery"""
    logger = get_run_logger()
    client = get_bigquery_client()

    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_ID}.{VMP_EXPRESSED_AS_TABLE_SPEC.table_id}`
    ORDER BY ddd_comment, vmp_name
    """

    results = client.query(query).result()
    rows = [dict(row) for row in results]
    df = pd.DataFrame(rows)

    logger.info(f"Retrieved {len(df)} records from populated table for validation")
    return df

@flow(name="Populate DDD Expressed As Table")
def populate_ddd_expressed_as_table():
    logger = get_run_logger()
    logger.info("Populating DDD Expressed As table")

    sql_file_path = Path(__file__).parent / "populate_ddd_expressed_as.sql"

    sql_result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD Expressed As table populated successfully")

    schema_validation = validate_table_schema(VMP_EXPRESSED_AS_TABLE_SPEC)
    logger.info("DDD Expressed As table schema validation completed")

    df = get_populated_data()

    validate_expressed_as_vmps(df)
    validate_expressed_as_units(df)
    validate_expressed_as_ingredients(df)
    validate_all_expressed_as_vmps_covered(df)

    logger.info("All validations passed successfully")


if __name__ == "__main__":
    populate_ddd_expressed_as_table() 