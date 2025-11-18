from google.cloud import bigquery
from prefect import task, flow, get_run_logger
from pipeline.setup.config import PROJECT_ID, DATASET_ID, VMP_TABLE_ID
from pipeline.utils.utils import get_bigquery_client
from pipeline.setup.bq_tables import VMP_EXPRESSED_AS_TABLE_SPEC
from pathlib import Path
import pandas as pd


@task
def load_expressed_as_from_csv(csv_path: Path) -> pd.DataFrame:
    """Load and validate CSV data"""
    logger = get_run_logger()
    
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)

    required_cols = [
        "vmp_id",
        "vmp_name",
        "ddd_comment",
        "expressed_as_strnt_nmrtr",
        "expressed_as_strnt_nmrtr_uom",
        "expressed_as_strnt_nmrtr_uom_name",
        "ingredient_code",
        "ingredient_name"
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df["vmp_id"] = df["vmp_id"].astype(str)
    df["ingredient_code"] = df["ingredient_code"].astype(str)
    df["expressed_as_strnt_nmrtr"] = df["expressed_as_strnt_nmrtr"].astype(str)
    df["expressed_as_strnt_nmrtr"] = pd.to_numeric(df["expressed_as_strnt_nmrtr"], errors="coerce")
    
    logger.info(f"Loaded {len(df)} records from CSV")
    return df


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
    return df


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
    return df


@task
def validate_expressed_as_ingredients(df: pd.DataFrame) -> pd.DataFrame:
    """Validate that ingredient codes exist in VMP ingredients and names match"""
    logger = get_run_logger()
    client = get_bigquery_client()

    vmp_query = f"""
    SELECT 
        vmp_code,
        ARRAY_AGG(STRUCT(ing.ingredient_code, ing.ingredient_name)) AS ingredients
    FROM `{PROJECT_ID}.{DATASET_ID}.{VMP_TABLE_ID}` vmp,
    UNNEST(vmp.ingredients) AS ing
    WHERE vmp_code IN UNNEST(@vmp_codes)
    GROUP BY vmp_code
    """

    vmp_codes = df["vmp_id"].astype(str).unique().tolist()
    if not vmp_codes:
        logger.warning("No VMP codes to validate")
        return df
    
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
    
    for idx, row in df.iterrows():
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
    return df


@task
def load_to_bigquery(table_id: str, df: pd.DataFrame) -> None:
    """Import data to BigQuery"""
    logger = get_run_logger()
    client = get_bigquery_client()

    records = df.to_dict('records')
    
    job_config = bigquery.LoadJobConfig(
        schema=VMP_EXPRESSED_AS_TABLE_SPEC.schema,
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info(f"Uploading {len(records)} records to BigQuery...")
    job = client.load_table_from_json(records, table_id, job_config=job_config)
    job.result()
    logger.info(f"Successfully imported {len(records)} records to {table_id}")


@flow(name="Import Expressed As")
def import_expressed_as():
    """Import expressed as data into BigQuery from CSV."""
        
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{VMP_EXPRESSED_AS_TABLE_SPEC.table_id}"
    csv_path = Path(__file__).parent / "vmp_expressed_as.csv"
    
    df = load_expressed_as_from_csv(csv_path)
    validated_vmp_df = validate_expressed_as_vmps(df)
    validated_units_df = validate_expressed_as_units(validated_vmp_df)
    validated_df = validate_expressed_as_ingredients(validated_units_df)
    load_to_bigquery(table_id, validated_df)


if __name__ == "__main__":
    import_expressed_as()
