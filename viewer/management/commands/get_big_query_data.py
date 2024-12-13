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
    help = "Gets data from BigQuery"

    def handle(self, *args, **options):
        client = get_bigquery_client()
        self.get_and_save_table(client, "ingredient", self.ingredient_table_sql)
        self.get_and_save_table(
            client,
            "organisation",
            self.organisation_table_sql)
        self.get_and_save_table(client, "vtm", self.vtm_table_sql)
        self.get_and_save_table(client, "vmp", self.vmp_table_sql)
        self.get_and_save_partitioned_table(client, "dose", self.dose_table_sql, "dose_quantity")
        self.get_and_save_partitioned_table(client, "ingredient_quantity", self.ingredient_quantity_table_sql, "ingredient_quantity")
        self.get_and_save_table(client, "atc", self.atc_table_sql)
        self.get_and_save_table(client, "ddd", self.ddd_table_sql)
        self.get_and_save_table(client, "atc_mapping", self.atc_vmp_table_sql)
        self.get_and_save_table(client, "vmp_form", self.vmp_form_sql)
        self.get_and_save_table(client, "vmp_ontform", self.vmp_ontform_sql)
        self.get_and_save_table(client, "data_status", self.data_status_table_sql)
        self.get_and_save_table(client, "route", self.route_table_sql)

    def get_and_save_table(self, client, table_name, sql_query):
        print(f"Getting {table_name} table")
        df = execute_query(client, sql_query)
        df.to_csv(DATA_DIR / f"{table_name}_table.csv", index=False)

    def get_and_save_partitioned_table(
            self, client, table_name, sql_query, folder):
        print(f"Getting {table_name} table")
        partitions = self.get_partitions(client, table_name)

        for partition in tqdm(partitions):
            query = sql_query.format(partition=partition)
            df = execute_query(client, query)
            df.to_csv(
                DATA_DIR /
                folder /
                f"{table_name}_table_{partition}.csv",
                index=False)

    def get_partitions(self, client, table_name):
        query = f"""
        SELECT DISTINCT year_month
        FROM `ebmdatalab.scmd.{table_name}`
        ORDER BY year_month
        """
        df = execute_query(client, query)
        return df["year_month"].tolist()

    ingredient_table_sql = """
        SELECT DISTINCT ingredient_code, ing_nm
        FROM `ebmdatalab.scmd.ingredient_quantity`
    """

    organisation_table_sql = """
        SELECT DISTINCT d.ods_code, o.ods_name, o.region, o.icb, o.successor_ods_code
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

    atc_table_sql = """
        SELECT DISTINCT
            atc_code,
            name
        FROM `ebmdatalab.scmd.atc`
    """

    ddd_table_sql = """
        SELECT DISTINCT
            d.atc_code,
            CASE
                WHEN uc.conversion_factor IS NOT NULL THEN d.ddd * uc.conversion_factor
                ELSE d.ddd
            END AS ddd,
            COALESCE(uc.basis, d.unit_type) AS unit_type,
            d.adm_code
        FROM `ebmdatalab.scmd.ddd` d
        LEFT JOIN `ebmdatalab.scmd.unit_conversion` uc
            ON LOWER(d.unit_type) = uc.unit
    """

    atc_vmp_table_sql = """
        SELECT DISTINCT
            vmp_code,
            bnf_code,
            atc_code
        FROM `ebmdatalab.scmd.scmd_dmd_supp`
    """

    vmp_form_sql = """
        SELECT DISTINCT
            vmp_code,
            dform_form
        FROM `ebmdatalab.scmd.scmd_dmd`
    """

    vmp_ontform_sql = """
        SELECT DISTINCT v.id AS vmp, f.descr
FROM `ebmdatalab.dmd.vmp_full` v
        LEFT JOIN `ebmdatalab.dmd.ont` o ON o.vmp = v.id
        LEFT JOIN `ebmdatalab.dmd.ontformroute` f ON f.cd = o.form
    """

    data_status_table_sql = """
        SELECT DISTINCT year_month, file_type
        FROM `ebmdatalab.scmd.data_status`
    """

    route_table_sql = """
        SELECT DISTINCT
            vmp_code,
            route_cd,
            route_descr
        FROM `ebmdatalab.scmd.scmd_dmd_route`
    """
