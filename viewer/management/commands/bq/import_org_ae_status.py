import requests
import re
import io
import pandas as pd

from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from django.core.management.base import BaseCommand

from viewer.management.utils import get_bigquery_client


PROJECT_ROOT = Path(__file__).parent.parent
PROJECT_ID = "ebmdatalab"
DATASET_ID = "scmd"
TABLE_ID = "org_ae_status"


class Command(BaseCommand):
    help = "Imports A&E status data for organisations into BigQuery"

    def handle(self, *args, **options):
        df = self.fetch_ae_data()
        self.upload_bq(df)
        self.stdout.write(self.style.SUCCESS("Successfully imported A&E status data into BigQuery"))

    def fetch_ae_data(self):
        base_url = "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/"

        response = requests.get(base_url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all relevant yearly links
        links = soup.find_all('a', href=lambda href: href and 'ae-attendances-and-emergency-admissions' in href)

        all_data = []

        for link in links:
            year_url = link['href']
            year_response = requests.get(year_url)
            year_soup = BeautifulSoup(year_response.content, 'html.parser')

            # Find monthly CSV files - csv files availale from April 2018.
            csv_links = year_soup.find_all('a', href=lambda href: href and href.endswith('.csv') and 'Monthly-AE' in href)

            for csv_link in csv_links:
                file_url = csv_link['href']
                file_url = urljoin(year_url, file_url)

                file_response = requests.get(file_url)

                # get year month from file_url
                pattern = r"Monthly-AE-(\w+)-(\d{4})"
                year = re.search(pattern, file_url).group(1)
                month = re.search(pattern, file_url).group(2)
                year_month = pd.to_datetime(f"{year}-{month}")

                # if file is before 2020-09, then we need to use different columns
                if year_month < pd.to_datetime("2020-09"):
                    columns = [
                        "Org Code",
                        "Number of A&E attendances Type 1",
                    ]
                else:
                    columns = [
                        "Org Code",
                        "A&E attendances Type 1",
                    ]

                if year_month == pd.to_datetime("2020-08"):
                    # the Period column header is missing from the file. Include the first column and other named columns
                    df = pd.read_csv(
                        io.StringIO(file_response.text),
                        usecols=[0, 1, 4],
                        names = columns,
                        )
                else:
                    df = pd.read_csv(
                        io.StringIO(file_response.text),
                        usecols=columns,
                        )

                    # drop the bottom row - this contains the total
                    df = df.drop(df.index[-1])

                df['Period'] = year_month
                all_data.append(df)

        combined_data = pd.concat(all_data, ignore_index=True)

        # create binary indicator for each org if they have over 0 A&E attendances Type 1
        combined_data['has_ae'] = combined_data['A&E attendances Type 1'] > 0

        combined_data = combined_data[["Period", "Org Code", "has_ae"]]
        combined_data = combined_data.rename(columns={"Org Code": "ods_code", "Period": "period"})

        return combined_data

    def upload_bq(self, df: pd.DataFrame) -> None:
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        client = get_bigquery_client()

        dtypes = {
            "ods_code": str,
            "period": "datetime64[ns]",
            "has_ae": bool,
        }

        df = df.astype(dtypes)

        schema = [
            bigquery.SchemaField(
                "ods_code", "STRING", description="ODS code of the organisation"
            ),
            bigquery.SchemaField(
                "period", "TIMESTAMP", description="Period of the data"
            ),
            bigquery.SchemaField(
                "has_ae",
                "BOOLEAN",
                description="Binary indicator for each org if they have over 0 A&E attendances Type 1",
            ),
        ]

        try:
            table = client.get_table(full_table_id)
            self.stdout.write(f"Table {full_table_id} already exists with {table.num_rows} rows.")
        except NotFound:
            table = bigquery.Table(full_table_id, schema=schema)
            table.description = "A&E attendances Type 1 by organisation and month"
            table = client.create_table(table)
            self.stdout.write(f"Table {full_table_id} created.")

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)

        job.result()

        self.stdout.write(f"Loaded {job.output_rows} rows into {full_table_id}")


if __name__ == "__main__":
    df = fetch_ae_data()
    upload_bq(df)
