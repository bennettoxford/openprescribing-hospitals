import pandas as pd
import xml.etree.ElementTree as ET

from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from django.core.management.base import BaseCommand

from viewer.management.utils import get_bigquery_client, PROJECT_ID

DATASET_ID = "scmd"

class Command(BaseCommand):
    help = "Imports DDD and ATC data into BigQuery"

    def handle(self, *args, **options):
        client = get_bigquery_client()

        atc_path = Path(__file__).parent.parent / "data/atc/2024_ATC.xml"
        ddd_path = Path(__file__).parent.parent / "data/atc/2024 ATC_ddd.xml"

        atc_data = self.parse_xml(atc_path, ["ATCCode", "Name", "Comment"])
        ddd_data = self.parse_xml(ddd_path, ["ATCCode", "DDD", "UnitType", "AdmCode", "DDDComment"])

        atc_df = pd.DataFrame(atc_data, columns=["atc_code", "name", "comment"])
        atc_df = atc_df.astype({"atc_code": "string", "name": "string", "comment": "string"})

        ddd_df = pd.DataFrame(ddd_data, columns=["atc_code", "ddd", "unit_type", "adm_code", "ddd_comment"])
        ddd_df["ddd"] = pd.to_numeric(ddd_df["ddd"], errors="coerce")
        ddd_df = ddd_df.astype(
            {
                "atc_code": "string",
                "unit_type": "string",
                "adm_code": "string",
                "ddd_comment": "string",
            }
        )

        self.upload_bq(atc_df, ddd_df, client)
        self.stdout.write(self.style.SUCCESS("Successfully imported DDD and ATC data into BigQuery"))

    def parse_xml(self, file_path, fields):
        tree = ET.parse(file_path)
        root = tree.getroot()
        data = []
        for row in root.findall(".//z:row", namespaces={"z": "#RowsetSchema"}):
            data.append(tuple(row.get(field) for field in fields))
        return data

    def upload_bq(self, atc_df: pd.DataFrame, ddd_df: pd.DataFrame, client: bigquery.Client) -> None:
        atc_table_id = f"{PROJECT_ID}.{DATASET_ID}.atc"
        ddd_table_id = f"{PROJECT_ID}.{DATASET_ID}.ddd"

        atc_schema = [
            bigquery.SchemaField("atc_code", "STRING", description="ATC code"),
            bigquery.SchemaField("name", "STRING", description="Name of the ATC code"),
            bigquery.SchemaField("comment", "STRING", description="Comment of the ATC code"),
        ]

        ddd_schema = [
            bigquery.SchemaField("atc_code", "STRING", description="ATC code"),
            bigquery.SchemaField("ddd", "FLOAT", description="DDD value"),
            bigquery.SchemaField("unit_type", "STRING", description="Unit type"),
            bigquery.SchemaField("adm_code", "STRING", description="ADM code"),
            bigquery.SchemaField("ddd_comment", "STRING", description="DDD comment"),
        ]

        self.create_or_update_table(client, atc_table_id, atc_schema, atc_df, "ATC codes")
        self.create_or_update_table(client, ddd_table_id, ddd_schema, ddd_df, "DDD values")

    def create_or_update_table(self, client, table_id, schema, df, description):
        try:
            table = client.get_table(table_id)
            self.stdout.write(f"Table {table_id} already exists with {table.num_rows} rows.")
        except NotFound:
            table = bigquery.Table(table_id, schema=schema)
            table.description = description
            table = client.create_table(table)
            self.stdout.write(f"Table {table_id} created.")

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        self.stdout.write(f"Loaded {job.output_rows} rows into {table_id}")
