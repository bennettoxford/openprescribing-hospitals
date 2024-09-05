import os
from django.core.management.base import BaseCommand
from django.conf import settings
from google.cloud import bigquery

import pandas as pd
from tqdm import tqdm

from google.oauth2 import service_account

from pathlib import Path


PROJECT_ID = "ebmdatalab"
CREDENTIALS_FILE = Path(settings.BASE_DIR, "bq-service-account.json")
DATA_DIR = Path(settings.BASE_DIR, "data")
DATA_DIR.mkdir(exist_ok=True)

credentials = service_account.Credentials.from_service_account_file(
    os.path.join(settings.BASE_DIR, "bq-service-account.json"),
)

def get_bigquery_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

def execute_query(client, query):
    try:
        query_job = client.query(query)
        query_result = query_job.result()

        schema = [field.name for field in query_result.schema]
        rows = [dict(row.items()) for row in query_result]
        rows_df = pd.DataFrame(rows, columns=schema)
        return rows_df
    except Exception as e:
        print(f"Error executing query: {e}")


class Command(BaseCommand):
    help = 'Gets data from BigQuery'

    def handle(self, *args, **options):
        client = get_bigquery_client()
        # self.get_and_save_table(client, "ingredient", self.ingredient_table_sql)
        self.get_and_save_table(client, "organisation", self.organisation_table_sql)
        # self.get_and_save_table(client, "vtm", self.vtm_table_sql)
        # self.get_and_save_table(client, "vmp", self.vmp_table_sql)
        # self.get_and_save_partitioned_table(client, "dose", self.dose_table_sql, "dose_quantity")
        # self.get_and_save_partitioned_table(client, "ingredient_quantity", self.ingredient_quantity_table_sql, "ingredient_quantity")

    def get_and_save_table(self, client, table_name, sql_query):
        print(f"Getting {table_name} table")
        df = execute_query(client, sql_query)
        df.to_csv(DATA_DIR / f"{table_name}_table.csv", index=False)
    
    def get_and_save_partitioned_table(self, client, table_name, sql_query, folder):
        print(f"Getting {table_name} table")
        partitions = self.get_partitions(client, table_name)
        
        for partition in tqdm(partitions):
            query = sql_query.format(partition=partition)
            df = execute_query(client, query)
            df.to_csv(DATA_DIR / folder / f"{table_name}_table_{partition}.csv", index=False)
        

    def get_partitions(self, client, table_name):
        query = f"""
        SELECT DISTINCT year_month
        FROM `ebmdatalab.scmd.{table_name}`
        ORDER BY year_month
        """
        df = execute_query(client, query)
        return df['year_month'].tolist()

    ingredient_table_sql = """
        SELECT DISTINCT ingredient_code, ing_nm
        FROM `ebmdatalab.scmd.ingredient_quantity`
    """

    organisation_table_sql = """
        SELECT DISTINCT d.ods_code, o.ods_name, o.region, o.successor_ods_code
        FROM `ebmdatalab.scmd.dose` d
        LEFT JOIN `ebmdatalab.scmd.ods_mapped` o
        ON d.ods_code = o.ods_code
    """

    vtm_table_sql = """
        SELECT DISTINCT
            s.vtm,
            s.vtm_name,
            COALESCE(d.vmp_code, s.vmp_code) AS vmp_code,
            s.vmp_name,
            s.ing AS ingredient
        FROM `ebmdatalab.scmd.dose` d
        LEFT JOIN `ebmdatalab.scmd.scmd_dmd` s 
            ON d.vmp_code = COALESCE(s.vmp_code, s.vmp_code_prev)
    """

    vmp_table_sql = """
        SELECT DISTINCT
            s.vtm,
            COALESCE(d.vmp_code, s.vmp_code) AS vmp_code,
            s.vmp_name,
            s.ing AS ingredient
        FROM `ebmdatalab.scmd.dose` d
        LEFT JOIN `ebmdatalab.scmd.scmd_dmd` s 
            ON d.vmp_code = COALESCE(s.vmp_code, s.vmp_code_prev)
    """

    dose_table_sql = """
        SELECT year_month, vmp_code, dose_quantity, dose_unit, ods_code, logic,
               SCMD_quantity, SCMD_quantity_basis
        FROM `ebmdatalab.scmd.dose`
        WHERE year_month = '{partition}'
    """

    ingredient_quantity_table_sql = """
        SELECT year_month, ingredient_code, vmp_code, ingredient_quantity,
               ingredient_unit, ods_code, logic
        FROM `ebmdatalab.scmd.ingredient_quantity`
        WHERE year_month = '{partition}'
    """
