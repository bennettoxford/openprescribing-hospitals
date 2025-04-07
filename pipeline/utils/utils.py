from google.cloud import bigquery
from prefect import get_run_logger
from prefect_gcp import GcpCredentials
from jinja2 import Template
from pipeline.utils import config

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
