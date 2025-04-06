import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path
from google.cloud import storage
from google.api_core.exceptions import NotFound
from prefect import task, flow
from prefect.logging import get_run_logger
from google.cloud import bigquery

from pipeline.utils.utils import get_bigquery_client
from pipeline.utils.config import PROJECT_ID, DATASET_ID
from pipeline.bq_tables import WHO_ATC_TABLE_SPEC, WHO_DDD_TABLE_SPEC

BUCKET_NAME = "ebmdatalab"


@task
def download_from_gcs(bucket_name: str, source_blob_name: str, temp_dir: Path) -> Path:
    """Download file from GCS to temp directory"""
    logger = get_run_logger()
    logger.info(f"Downloading {source_blob_name} from GCS")

    bq_client = get_bigquery_client()
    storage_client = storage.Client(
        project=bq_client.project, credentials=bq_client._credentials
    )

    temp_dir.mkdir(parents=True, exist_ok=True)

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    destination_file = temp_dir / source_blob_name.split("/")[-1]
    blob.download_to_filename(destination_file)

    logger.info(f"Downloaded to {destination_file}")
    return destination_file


@task
def parse_xml(file_path: Path) -> pd.DataFrame:
    """Parse XML file and return a pandas DataFrame"""
    logger = get_run_logger()
    logger.info(f"Parsing {file_path}")

    tree = ET.parse(file_path)
    root = tree.getroot()

    data = []

    for row in root.findall(".//z:row", namespaces={"z": "#RowsetSchema"}):
        row_data = row.attrib
        data.append(row_data)

    return pd.DataFrame(data)


@task
def load_to_bigquery(df: pd.DataFrame, table_spec):
    """Load DataFrame to BigQuery"""
    logger = get_run_logger()
    logger.info(f"Loading data to {table_spec.full_table_id}")

    client = get_bigquery_client()

    job_config = bigquery.LoadJobConfig(
        schema=table_spec.schema, write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        df, table_spec.full_table_id, job_config=job_config
    )
    job.result()

    logger.info(f"Loaded {len(df)} rows to {table_spec.full_table_id}")


@task
def cleanup_temp_files(temp_dir: Path):
    """Clean up temporary files"""
    logger = get_run_logger()
    logger.info("Cleaning up temporary files")
    if temp_dir.exists():
        for file in temp_dir.glob("*"):
            file.unlink()
        temp_dir.rmdir()


@task
def table_has_data(table_spec) -> bool:
    """Check if a BigQuery table has data and exists"""
    logger = get_run_logger()
    client = get_bigquery_client()

    try:
        query = f"SELECT COUNT(*) as count FROM `{table_spec.full_table_id}`"
        result = client.query(query).result()
        count = list(result)[0].count
        logger.info(f"Table {table_spec.full_table_id} has {count} rows")
        return count > 0
    except NotFound:
        raise RuntimeError(
            f"Table {table_spec.full_table_id} does not exist but should!"
        )


@task
def validate_who_routes(df: pd.DataFrame):
    """Validate that DDD adm_codes exist in who_routes_of_administration table"""
    logger = get_run_logger()
    client = get_bigquery_client()

    ddd_routes = set(df["adm_code"].dropna().unique())

    query = f"""
    WITH ddd_routes AS (
        SELECT code as adm_code
        FROM UNNEST({list(ddd_routes)}) as code
    )
    SELECT d.adm_code
    FROM ddd_routes d
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.who_routes_of_administration` who
        ON d.adm_code = who.who_route_code
    WHERE who.who_route_code IS NULL
    """

    results = client.query(query).result()

    missing_routes = [row.adm_code for row in results]

    if missing_routes:
        error_msg = "Route validation failed. The following routes are not in who_routes_of_administration table:\n"
        error_msg += "\n".join(f"- {route}" for route in missing_routes)
        logger.error(error_msg)
        raise ValueError("Invalid route codes detected")

    logger.info("All DDD administration routes are present in WHO routes table")


@flow(name="Import DDD and ATC Data")
def import_ddd_atc_flow():
    """Main flow to import DDD and ATC data into BigQuery"""
    logger = get_run_logger()
    logger.info("Starting DDD and ATC data import flow")

    atc_has_data = table_has_data(WHO_ATC_TABLE_SPEC)
    ddd_has_data = table_has_data(WHO_DDD_TABLE_SPEC)

    if atc_has_data and ddd_has_data:
        logger.info("Tables already populated, skipping import")
        return {"atc": True, "ddd": True}

    temp_dir = Path("temp/atc")
    results = {"atc": False, "ddd": False}

    try:
        atc_path = download_from_gcs(
            BUCKET_NAME, "who_atc_ddd_op_hosp/2024 ATC.xml", temp_dir
        )
        ddd_path = download_from_gcs(
            BUCKET_NAME, "who_atc_ddd_op_hosp/2024 ATC_ddd.xml", temp_dir
        )

        atc_df = parse_xml(atc_path)
        ddd_df = parse_xml(ddd_path)

        atc_column_mapping = {
            "ATCCode": "atc_code",
            "Name": "atc_name",
            "Comment": "comment",
        }

        ddd_column_mapping = {
            "ATCCode": "atc_code",
            "DDD": "ddd",
            "UnitType": "ddd_unit",
            "AdmCode": "adm_code",
            "DDDComment": "comment",
        }

        atc_df = atc_df.rename(columns=atc_column_mapping)
        ddd_df = ddd_df.rename(columns=ddd_column_mapping)

        ddd_df["adm_code"] = ddd_df["adm_code"].str.strip()
        ddd_df["ddd_unit"] = ddd_df["ddd_unit"].str.lower()

        ddd_df["ddd"] = ddd_df["ddd"].apply(lambda x: float(x) if x else None)

        validate_who_routes(ddd_df)

        if not atc_has_data:
            load_to_bigquery(atc_df, WHO_ATC_TABLE_SPEC)
            results["atc"] = True
        if not ddd_has_data:
            load_to_bigquery(ddd_df, WHO_DDD_TABLE_SPEC)
            results["ddd"] = True

    finally:
        cleanup_temp_files(temp_dir)

    return results


if __name__ == "__main__":
    import_ddd_atc_flow()
