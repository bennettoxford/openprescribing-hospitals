from django.core.management.base import BaseCommand
from pathlib import Path

from viewer.management.utils import get_bigquery_client, PROJECT_ROOT


CREDENTIALS_FILE = PROJECT_ROOT / "bq-service-account.json"

class Command(BaseCommand):
    help = "Executes a SQL query from a file against BigQuery"

    def add_arguments(self, parser):
        parser.add_argument("--sql-file", type=str, required=True, help="Path to the SQL file to execute")

    def handle(self, *args, **options):
        sql_file = options['sql_file']
        client = get_bigquery_client()
        query = self.read_sql_file(Path(__file__).parent.parent / "sql" / f"{sql_file}")
        self.execute_query(client, query)

    def read_sql_file(self, file_path):
        with open(file_path, 'r') as file:
            return file.read()

    def execute_query(self, client, query):
        try:
            query_job = client.query(query)
            query_job.result()
            self.stdout.write(self.style.SUCCESS("Query executed successfully"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error executing query: {e}"))