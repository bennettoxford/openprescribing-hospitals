import re
import requests
import pandas as pd
import io

from typing import List, Tuple, Iterator
from google.cloud import bigquery as gcbq
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from django.core.management.base import BaseCommand


PROJECT_ROOT = Path(__file__).parent.parent
CREDENTIALS_FILE = PROJECT_ROOT / "bq-service-account.json"
PROJECT_ID = "ebmdatalab"
DATASET_ID = "scmd"
DATA_STATUS_TABLE_ID = "data_status"
SCMD_TABLE_ID = "scmd_latest"

class Command(BaseCommand):
    help = "Imports SCMD data into BigQuery"

    def handle(self, *args, **options):
        data_status_df, urls_by_month_and_file_type = create_data_status_df()
        importer = SCMDImporter(urls_by_month_and_file_type)
        importer.update_tables(data_status_df)
        self.stdout.write(self.style.SUCCESS("Successfully imported SCMD data"))

class SCMDImporter:
    def __init__(self, urls_by_month_and_file_type):
        self.client = self._get_bigquery_client()
        self.data_status_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{DATA_STATUS_TABLE_ID}"
        self.scmd_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{SCMD_TABLE_ID}"
        self.urls_by_month_and_file_type = urls_by_month_and_file_type

        self.data_status_table_schema = [
            bigquery.SchemaField("year_month", "DATE", description="Year and month of the data"),
            bigquery.SchemaField("file_type", "STRING", description="Type of the file"),
        ]
        self.scmd_table_schema = [
            bigquery.SchemaField("year_month", "DATE", description="Year and month of the data"),
            bigquery.SchemaField("ods_code", "STRING", description="ODS code"),
            bigquery.SchemaField("vmp_snomed_code", "INTEGER", description="SNOMED code for the product"),
            bigquery.SchemaField("vmp_product_name", "STRING", description="Product name"),
            bigquery.SchemaField("unit_of_measure_identifier", "INTEGER", description="Identifier for the unit of measure"),
            bigquery.SchemaField("unit_of_measure_name", "STRING", description="Name of the unit of measure"),
            bigquery.SchemaField("total_quanity_in_vmp_unit", "FLOAT", description="Total quantity in the unit of measure"),
            bigquery.SchemaField("indicative_cost", "FLOAT", description="Indicative cost"),
        ]

    def _get_bigquery_client(self) -> bigquery.Client:
        credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)

    def get_existing_data(self) -> pd.DataFrame:
        query = f"""
        SELECT DISTINCT year_month, file_type 
        FROM {self.data_status_table_full_id}
        ORDER BY year_month DESC
        """
        query_job = self.client.query(query)
        query_result = query_job.result()

        rows = {
        }
        for i in query_result:
            rows[i.year_month] = i.file_type

        rows_df = pd.DataFrame(rows.items(), columns=["year_month", "file_type"])
        rows_df["year_month"] = pd.to_datetime(rows_df["year_month"], format="%Y-%m-%d")

        return rows_df
        # return query_job.to_dataframe()
        # need read permissions for project

    def update_tables(self, new_status_data: pd.DataFrame):
        try:
            existing_data = self.get_existing_data()
            self._update_existing_tables(new_status_data, existing_data)
        except NotFound:
            self._create_new_tables(new_status_data)

    def _update_existing_tables(self, new_status_data: pd.DataFrame, existing_data: pd.DataFrame):
        new_months, out_of_date_months = self._get_months_to_update(new_status_data, existing_data)
        months_to_update = new_months + out_of_date_months

        print(f"Found {len(new_months)} new months:")
        print(new_months)
        print(f"Found {len(out_of_date_months)} out-of-date months:")
        print(out_of_date_months)

        # update the scmd data first. If there are any errors, the data status will not be updated
        self._update_scmd_data(months_to_update)
        self._update_data_status(new_status_data, months_to_update)
        

    def _get_months_to_update(self, new_data: pd.DataFrame, existing_data: pd.DataFrame) -> Tuple[List[pd.Timestamp], List[pd.Timestamp]]:
        merged_data = pd.merge(new_data, existing_data, on="year_month", how="left", suffixes=("_new", "_old"))
        
        new_months = merged_data[merged_data["file_type_old"].isna()]["year_month"].tolist()
        out_of_date_months = merged_data[
            (merged_data["file_type_old"].notna()) & (merged_data["file_type_new"] != merged_data["file_type_old"])
        ]["year_month"].tolist()
        
        return new_months, out_of_date_months

    def _update_data_status(self, new_data: pd.DataFrame, months_to_update: List[pd.Timestamp]):
        for month in months_to_update:
            month_str = month.strftime("%Y%m%d")
            row = new_data[new_data["year_month"] == month].copy()
            row["year_month"] = row["year_month"].dt.date

            job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            partition = f"{self.data_status_table_full_id}${month_str}"
            job = self.client.load_table_from_dataframe(row, partition, job_config=job_config)
            job.result()

    def _create_new_tables(self, status_data: pd.DataFrame):
        
        self._create_scmd_table()
        self._create_data_status_table(status_data)
        self._update_scmd_data(status_data['year_month'].tolist())
        

    def _create_data_status_table(self, status_data: pd.DataFrame):
        time_partitioning = gcbq.TimePartitioning(
            type_=gcbq.TimePartitioningType.DAY,
            field="year_month",
        )
        table = bigquery.Table(self.data_status_table_full_id, schema=self.data_status_table_schema)
        table.description = "Data status for the SCMD dataset"
        table.time_partitioning = time_partitioning
        self.client.create_table(table)
        print(f"Table {self.data_status_table_full_id} created.")

        job_config = bigquery.LoadJobConfig(
            schema=self.data_status_table_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = self.client.load_table_from_dataframe(status_data, self.data_status_table_full_id, job_config=job_config)
        job.result()

    def _create_scmd_table(self):
        table = bigquery.Table(self.scmd_table_full_id, schema=self.scmd_table_schema)
        table.description = "SCMD dataset"
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="year_month",
        )
        table.clustering_fields = ["vmp_snomed_code"]
        self.client.create_table(table)
        print(f"Table {self.scmd_table_full_id} created.")

    def _update_scmd_data(self, months_to_update: List[pd.Timestamp]):
        for month in months_to_update:
            scmd_data = self._fetch_scmd_data(month)
            converted_data = self._convert_units(scmd_data)
            self._load_scmd_data(converted_data, month)

    def _fetch_scmd_data(self, month: pd.Timestamp) -> pd.DataFrame:
        month_str = month.strftime("%Y-%m-%d")
        url = self.urls_by_month_and_file_type[month_str]['url']
        
        response = requests.get(url)
        response.raise_for_status()
        
        csv_content = io.StringIO(response.text)
        df = pd.read_csv(csv_content)

      
        
        # Add the year_month column
        df['YEAR_MONTH'] = pd.to_datetime(month_str)
        
        # Rename columns to match the schema
        column_mapping = {
            'YEAR_MONTH': 'year_month',
            'ODS_CODE': 'ods_code',
            'VMP_SNOMED_CODE': 'vmp_snomed_code',
            'VMP_PRODUCT_NAME': 'vmp_product_name',
            'UNIT_OF_MEASURE_IDENTIFIER': 'unit_of_measure_identifier',
            'UNIT_OF_MEASURE_NAME': 'unit_of_measure_name',
            'TOTAL_QUANITY_IN_VMP_UNIT': 'total_quanity_in_vmp_unit',
            'INDICATIVE_COST': 'indicative_cost'
        }

        df.rename(columns=column_mapping, inplace=True)
        df['unit_of_measure_name'] = df['unit_of_measure_name'].str.lower()
        df['vmp_snomed_code'] = df['vmp_snomed_code'].astype('int64')
        df['unit_of_measure_identifier'] = df['unit_of_measure_identifier'].astype('int64')
        df['total_quanity_in_vmp_unit'] = df['total_quanity_in_vmp_unit'].astype(float)
        df['indicative_cost'] = df['indicative_cost'].astype(float)
        
        return df
    
    def _convert_units(self, df: pd.DataFrame) -> pd.DataFrame:
        # Fetch unit conversion data
        unit_conversion_query = """
        SELECT unit, basis, CAST(conversion_factor AS FLOAT64) AS conversion_factor, unit_id, basis_id
        FROM `ebmdatalab.scmd.unit_conversion`
        """
        unit_conversion_df = self.client.query(unit_conversion_query)

        query_result = unit_conversion_df.result()

        rows = []
        for i in query_result:
            rows.append({
                "unit": i.unit,
                "basis": i.basis,
                "conversion_factor": i.conversion_factor,
                "unit_id": i.unit_id,
                "basis_id": i.basis_id
            })

        unit_conversion_df = pd.DataFrame(rows)  
        unit_conversion_df['unit_id'] = unit_conversion_df['unit_id'].astype('int64')

        # there are some rows in SCMD data where the unit of measure identifier is missing. We should find out why.
        if df[df['unit_of_measure_identifier'].isna()].shape[0] > 0:
            print(f"Dropping {df[df['unit_of_measure_identifier'].isna()].shape[0]} rows where the unit of measure identifier is missing.")
            df = df[df['unit_of_measure_identifier'].notna()]

        df = pd.merge(df, unit_conversion_df, left_on='unit_of_measure_identifier', right_on='unit_id', how='left')
        
        # Perform unit conversions
        df['quantity_in_basis_unit'] = df['total_quanity_in_vmp_unit'] * df['conversion_factor']

   
        nan_count = df['quantity_in_basis_unit'].isna().sum()
        if nan_count > 0:
            print(df[df['conversion_factor'].isna()]["unit_of_measure_identifier"])
            raise ValueError(f"Found {nan_count} NaN values in 'quantity_in_basis_unit' column after conversion.")
        
        # now replace the total_quanity_in_vmp_unit with the quantity_in_basis_unit
        df['total_quanity_in_vmp_unit'] = df['quantity_in_basis_unit']
        df['unit_of_measure_identifier'] = df['basis_id']
        df['unit_of_measure_name'] = df['basis']

        df = df.drop(columns=['unit', 'basis', 'conversion_factor', 'unit_id', 'basis_id', 'quantity_in_basis_unit'])

        return df

    def _load_scmd_data(self, scmd_data: pd.DataFrame, month: pd.Timestamp):
        month_str = month.strftime("%Y%m%d")
        job_config = bigquery.LoadJobConfig(
            schema=self.scmd_table_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            clustering_fields=["vmp_snomed_code"],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="year_month",
            )
        )
        partition = f"{self.scmd_table_full_id}${month_str}"
        job = self.client.load_table_from_dataframe(scmd_data, partition, job_config=job_config)
        job.result()

def iter_dataset_urls() -> Iterator[str]:
    """Extract CSV file URLs via the API"""
    dataset_name = "secondary-care-medicines-data-indicative-price"
    dataset_url = f"https://opendata.nhsbsa.net/api/3/action/package_show?id={dataset_name}"

    response = requests.get(dataset_url)
    response.raise_for_status()

    data = response.json()
    resources = data["result"]["resources"]

    pattern = r"scmd_(final|provisional|wip)_[0-9]{6}\.csv"

    return (
        resource["url"]
        for resource in resources
        if resource["format"].upper() == "CSV" and re.search(pattern, resource["url"].split("/")[-1])
    )

def iter_months(urls: Iterator[str]) -> Iterator[Tuple[str, str, str]]:
    """Extract a "month" and file type from each URL given."""
    pattern = r"scmd_(final|provisional|wip)_([0-9]{4})([0-9]{2})\.csv"
    for url in urls:
        match = re.search(pattern, url.split("/")[-1])
        if match:
            file_type, year, month = match.groups()
            yield f"{year}-{month}", file_type, url
        else:
            raise ValueError(f"Unexpected URL format: {url}")

def create_data_status_df() -> Tuple[pd.DataFrame, dict]:
    urls_by_month_and_file_type = {
        f"{month[:4]}{month[4:]}-01": {"url": url, "file_type": file_type}
        for month, file_type, url in iter_months(iter_dataset_urls())
    }

    data_status_df = pd.DataFrame(
        [(month, details["file_type"]) for month, details in urls_by_month_and_file_type.items()],
        columns=["year_month", "file_type"],
    ).sort_values(by="year_month")

    data_status_df["year_month"] = pd.to_datetime(data_status_df["year_month"])

    return data_status_df, urls_by_month_and_file_type