from google.cloud import bigquery
from prefect import task, flow, get_run_logger
from pipeline.utils.config import PROJECT_ID, DATASET_ID
from pipeline.utils.utils import get_bigquery_client
from pipeline.bq_tables import VMP_EXPRESSED_AS_TABLE_SPEC
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
        "expressed_as_strnt_nmrtr_uom_name"
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df["vmp_id"] = df["vmp_id"].astype(str)
    df["expressed_as_strnt_nmrtr"] = df["expressed_as_strnt_nmrtr"].astype(str)
    df["expressed_as_strnt_nmrtr"] = pd.to_numeric(df["expressed_as_strnt_nmrtr"], errors="coerce")
    
    logger.info(f"Loaded {len(df)} records from CSV")
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
def import_expressed_as(table_id: str, df: pd.DataFrame) -> None:
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
def import_expressed_as_flow(csv_path: Path):
    """Import expressed as data into BigQuery from CSV."""
        
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{VMP_EXPRESSED_AS_TABLE_SPEC.table_id}"
    
    df = load_expressed_as_from_csv(csv_path)
    validated_df = validate_expressed_as_units(df)
    import_expressed_as(table_id, validated_df)


if __name__ == "__main__":
    csv_path = Path(__file__).parent.parent / "data" / "vmp_expressed_as.csv"
    import_expressed_as_flow(csv_path)
