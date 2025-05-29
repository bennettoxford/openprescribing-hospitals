from google.cloud import bigquery
from prefect import get_run_logger, task
from prefect_gcp import GcpCredentials
from jinja2 import Template
from pipeline.utils import config
from pathlib import Path
import pandas as pd
import xml.etree.ElementTree as ET
from google.cloud import storage

def get_bigquery_client() -> bigquery.Client:
    """Create and return a BigQuery client"""
    credentials = GcpCredentials.load("bq")
    google_credentials = credentials.get_credentials_from_service_account()
    return bigquery.Client(
        project=config.PROJECT_ID,
        credentials=google_credentials,
        location='EU'
    )


def execute_bigquery_query_from_sql_file(sql_file: str):
    """Execute a BigQuery query from a SQL file with Jinja templating support.
    
    Args:
        sql_file: Path to the SQL file
        
    Returns:
        Query results
    """
    try:
        client = get_bigquery_client()

        with open(sql_file, 'r') as file:
            template = Template(file.read())
            sql_query = template.render(
                **{k: v for k, v in vars(config).items() if not k.startswith('_')}
            )
        
        query_job = client.query(sql_query)
        return query_job.result()
    except Exception as e:
        raise e


def validate_table_schema(table_spec):
    """Validate that a table's schema matches the expected schema defined in TableSpec.
    
    Args:
        table_spec: The TableSpec instance containing the expected schema
        
    Returns:
        bool: True if schema matches, False otherwise
    """
    logger = get_run_logger()
    client = get_bigquery_client()
    
    logger.info(f"Validating schema for table {table_spec.full_table_id}")
    
    try:
        table_ref = client.dataset(table_spec.dataset_id).table(table_spec.table_id)
        table = client.get_table(table_ref)
        
        table_schema_names = {field.name: field.field_type for field in table.schema}
        expected_schema_names = {field.name: field.field_type for field in table_spec.schema}
        
        missing_fields = [f for f in expected_schema_names if f not in table_schema_names]
        extra_fields = [f for f in table_schema_names if f not in expected_schema_names]
        mismatched_types = [f for f in expected_schema_names if f in table_schema_names 
                           and table_schema_names[f] != expected_schema_names[f]]
        
        if missing_fields or extra_fields or mismatched_types:
            logger.warning(f"Schema mismatch detected for {table_spec.full_table_id}:")
            if missing_fields:
                logger.warning(f"Missing fields: {missing_fields}")
            if extra_fields:
                logger.warning(f"Extra fields: {extra_fields}")
            if mismatched_types:
                logger.warning(f"Mismatched types: {mismatched_types}")
            return False
        
        logger.info(f"Schema validation successful for {table_spec.full_table_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error validating schema for {table_spec.full_table_id}: {str(e)}")
        return False

@task
def download_from_gcs(bucket_name: str, source_blob_name: str, temp_dir: Path) -> Path:
    """Download file from GCS to temp directory"""
    logger = get_run_logger()
    logger.info(f"Downloading {source_blob_name} from GCS")

    bq_client = get_bigquery_client()
    storage_client = storage.Client(
        project=bq_client.project, 
        credentials=bq_client._credentials
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
        data.append(row.attrib)

    return pd.DataFrame(data)


@task
def load_to_bigquery(df: pd.DataFrame, table_spec) -> None:
    """Load DataFrame to BigQuery"""
    logger = get_run_logger()
    logger.info(f"Loading data to {table_spec.full_table_id}")

    client = get_bigquery_client()
    job_config = bigquery.LoadJobConfig(
        schema=table_spec.schema, 
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        df, table_spec.full_table_id, job_config=job_config
    )
    job.result()

    logger.info(f"Loaded {len(df)} rows to {table_spec.full_table_id}")


@task
def cleanup_temp_files(temp_dir: Path) -> None:
    """Clean up temporary files"""
    logger = get_run_logger()
    logger.info("Cleaning up temporary files")
    if temp_dir.exists():
        for file in temp_dir.glob("*"):
            file.unlink()
        temp_dir.rmdir()


def fetch_table_data_from_bq(table_spec) -> pd.DataFrame:
    """Fetch data from a BigQuery table"""
    logger = get_run_logger()
    client = get_bigquery_client()
    
    logger.info(f"Fetching {table_spec.table_id} from BigQuery")
    table = client.dataset(table_spec.dataset_id).table(table_spec.table_id)
    table_ref = client.get_table(table)
    rows = client.list_rows(table_ref, selected_fields=table_ref.schema)
    
    data = [dict(row) for row in rows]
    df = pd.DataFrame(data)
    logger.info(f"Found {len(df)} {table_spec.table_id}")
    return df
