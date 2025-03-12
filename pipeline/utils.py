from google.cloud import bigquery
from prefect_gcp import GcpCredentials
from jinja2 import Template
import config


def get_bigquery_client() -> bigquery.Client:
    """Create and return a BigQuery client"""
    credentials = GcpCredentials.load("bq")
    google_credentials = credentials.get_credentials_from_service_account()
    return bigquery.Client(project=config.PROJECT_ID, credentials=google_credentials, location='EU')


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
            sql_query = template.render(**{k: v for k, v in vars(config).items() if not k.startswith('_')})
        
        query_job = client.query(sql_query)
        return query_job.result()
    except Exception as e:
        raise e
