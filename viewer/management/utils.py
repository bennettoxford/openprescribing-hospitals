from google.oauth2 import service_account
from google.cloud import bigquery
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
CREDENTIALS_FILE = PROJECT_ROOT / "bq-service-account.json"
PROJECT_ID = "ebmdatalab"

def get_bigquery_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

