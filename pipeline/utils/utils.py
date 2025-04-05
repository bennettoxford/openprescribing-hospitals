from google.cloud import bigquery
from prefect_gcp import GcpCredentials
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
