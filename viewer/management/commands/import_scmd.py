import logging
import re
import requests
import pandas as pd
import io

from typing import List, Iterator, Dict, Optional
from dataclasses import dataclass
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
from django.core.management.base import BaseCommand

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
CREDENTIALS_FILE = PROJECT_ROOT / "bq-service-account.json"
PROJECT_ID = "ebmdatalab"
DATASET_ID = "scmd"
DATA_STATUS_TABLE_ID = "data_status"
SCMD_TABLE_ID = "scmd_latest"

DATA_STATUS_SCHEMA = [
    bigquery.SchemaField(
        "year_month", "DATE", description="Year and month of the data"
    ),
    bigquery.SchemaField("file_type", "STRING", description="Type of the file"),
]

SCMD_TABLE_SCHEMA = [
    bigquery.SchemaField(
        "year_month", "DATE", description="Year and month of the data"
    ),
    bigquery.SchemaField("ods_code", "STRING", description="ODS code"),
    bigquery.SchemaField(
        "vmp_snomed_code", "INTEGER", description="SNOMED code for the product"
    ),
    bigquery.SchemaField("vmp_product_name", "STRING", description="Product name"),
    bigquery.SchemaField(
        "unit_of_measure_identifier",
        "INTEGER",
        description="Identifier for the unit of measure",
    ),
    bigquery.SchemaField(
        "unit_of_measure_name", "STRING", description="Name of the unit of measure"
    ),
    bigquery.SchemaField(
        "total_quanity_in_vmp_unit",
        "FLOAT",
        description="Total quantity in the unit of measure",
    ),
    bigquery.SchemaField("indicative_cost", "FLOAT", description="Indicative cost"),
]


@dataclass
class DatasetURL:
    month: str
    file_type: str
    url: str


class DataFetcher:
    """Fetches SCMD data URLs from the NHS BSA API."""

    @staticmethod
    def iter_dataset_urls() -> Iterator[str]:
        """Extract CSV file URLs via the API."""
        dataset_name = "secondary-care-medicines-data-indicative-price"
        dataset_url = (
            f"https://opendata.nhsbsa.net/api/3/action/package_show?id={dataset_name}"
        )

        response = requests.get(dataset_url)
        response.raise_for_status()

        data = response.json()
        resources = data["result"]["resources"]

        pattern = r"scmd_(final|provisional|wip)_[0-9]{6}\.csv"

        return (
            resource["url"]
            for resource in resources
            if resource["format"].upper() == "CSV"
            and re.search(pattern, resource["url"].split("/")[-1])
        )

    @staticmethod
    def iter_months(urls: Iterator[str]) -> Iterator[DatasetURL]:
        """Extract a "month" and file type from each URL given."""
        pattern = r"scmd_(final|provisional|wip)_([0-9]{4})([0-9]{2})\.csv"
        for url in urls:
            match = re.search(pattern, url.split("/")[-1])
            if match:
                file_type, year, month = match.groups()
                yield DatasetURL(month=f"{year}-{month}", file_type=file_type, url=url)
            else:
                raise ValueError(f"Unexpected URL format: {url}")


class UnitConverter:
    """Handles unit conversion for SCMD data."""

    def __init__(self, client: bigquery.Client):
        self.client = client

    def fetch_conversion_factors(self) -> pd.DataFrame:
        """Fetches unit conversion data from BigQuery."""
        query = """
        SELECT unit, basis, CAST(conversion_factor AS FLOAT64) AS conversion_factor, unit_id, basis_id
        FROM `ebmdatalab.scmd.unit_conversion`
        """
        query_result = self.client.query(query).result()

        rows = [
            {
                "unit": row.unit,
                "basis": row.basis,
                "conversion_factor": row.conversion_factor,
                "unit_id": row.unit_id,
                "basis_id": row.basis_id,
            }
            for row in query_result
        ]

        return pd.DataFrame(rows)

    def convert_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardises units using conversion factors from reference table."""
        unit_conversion_df = self.fetch_conversion_factors()
        unit_conversion_df["unit_id"] = unit_conversion_df["unit_id"].astype("int64")

        if df[df["unit_of_measure_identifier"].isna()].shape[0] > 0:
            logger.warning(
                f"Dropping {df[df['unit_of_measure_identifier'].isna()].shape[0]} rows where the unit of measure identifier is missing."
            )
            df = df[df["unit_of_measure_identifier"].notna()]

        df = pd.merge(
            df,
            unit_conversion_df,
            left_on="unit_of_measure_identifier",
            right_on="unit_id",
            how="left",
        )

        df["quantity_in_basis_unit"] = (
            df["total_quanity_in_vmp_unit"] * df["conversion_factor"]
        )

        nan_count = df["quantity_in_basis_unit"].isna().sum()
        if nan_count > 0:
            logger.error(
                f"Found {nan_count} NaN values in 'quantity_in_basis_unit' column after conversion."
            )
            raise ValueError(
                f"Found {nan_count} NaN values in 'quantity_in_basis_unit' column after conversion."
            )

        df["total_quanity_in_vmp_unit"] = df["quantity_in_basis_unit"]
        df["unit_of_measure_identifier"] = df["basis_id"]
        df["unit_of_measure_name"] = df["basis"]

        return df.drop(
            columns=[
                "unit",
                "basis",
                "conversion_factor",
                "unit_id",
                "basis_id",
                "quantity_in_basis_unit",
            ]
        )


class BigQueryTableManager:
    """Handles BigQuery table operations."""

    def __init__(
        self, client: bigquery.Client, table_id: str, schema: List[bigquery.SchemaField]
    ):
        self.client = client
        self.table_id = table_id
        self.schema = schema

    def create_table(
        self,
        description: str,
        partition_field: str,
        clustering_fields: Optional[List[str]] = None,
    ) -> None:
        """Creates a new BigQuery table with the specified configuration."""
        table = bigquery.Table(self.table_id, schema=self.schema)
        table.description = description
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        if clustering_fields:
            table.clustering_fields = clustering_fields

        self.client.create_table(table)
        logger.info(f"Created table: {self.table_id}")

    def load_dataframe(
        self,
        df: pd.DataFrame,
        partition_date: Optional[str] = None,
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> None:
        """Loads a DataFrame into the BigQuery table."""
        job_config = bigquery.LoadJobConfig(
            schema=self.schema,
            write_disposition=write_disposition,
        )

        target_table = (
            f"{self.table_id}${partition_date}" if partition_date else self.table_id
        )
        job = self.client.load_table_from_dataframe(
            df, target_table, job_config=job_config
        )
        job.result()
        logger.info(f"Loaded data into: {target_table}")


class SCMDImporter:
    """
    Handles the import of SCMD data into BigQuery.

    Manages two tables:
    1. Data status table: Tracks which months have been imported and their status (final, provisional, wip)
    2. SCMD table: Contains the actual medicines data. This is partitioned by year and month and clustered by VMP code.
    """

    def __init__(self, urls_by_month: Dict[str, DatasetURL]):
        self.urls_by_month = urls_by_month
        self.client = self._get_bigquery_client()

        self.data_status_manager = BigQueryTableManager(
            self.client,
            f"{PROJECT_ID}.{DATASET_ID}.{DATA_STATUS_TABLE_ID}",
            DATA_STATUS_SCHEMA,
        )
        self.scmd_manager = BigQueryTableManager(
            self.client, f"{PROJECT_ID}.{DATASET_ID}.{SCMD_TABLE_ID}", SCMD_TABLE_SCHEMA
        )
        self.unit_converter = UnitConverter(self.client)

    def _get_bigquery_client(self) -> bigquery.Client:
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE
        )
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)

    def _get_existing_data(self) -> pd.DataFrame:
        """
        Gets the existing data from the data status table.
        """
        query = f"""
        SELECT DISTINCT year_month, file_type 
        FROM {self.data_status_table_full_id}
        ORDER BY year_month DESC
        """
        logger.debug(f"Executing query: {query}")
        query_job = self.client.query(query)
        query_result = query_job.result()

        # Log the query result for debugging
        logger.debug(
            f"Query result: {[{'year_month': i.year_month, 'file_type': i.file_type} for i in query_result]}"
        )

        # This is a way around missing read permissions for the project.
        # Update to return query_job.to_dataframe() when permissions are fixed.
        rows = {i.year_month: i.file_type for i in query_result}

        rows_df = pd.DataFrame(rows.items(), columns=["year_month", "file_type"])
        rows_df["year_month"] = pd.to_datetime(rows_df["year_month"], format="%Y-%m-%d")

        return rows_df

    def _process_month_data(self, month: pd.Timestamp) -> pd.DataFrame:
        """Processes data for a single month."""
        month_str = month.strftime("%Y-%m-%d")
        url = self.urls_by_month[month_str].url

        response = requests.get(url)
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text))
        df["YEAR_MONTH"] = pd.to_datetime(month_str)

        column_mapping = {
            "YEAR_MONTH": "year_month",
            "ODS_CODE": "ods_code",
            "VMP_SNOMED_CODE": "vmp_snomed_code",
            "VMP_PRODUCT_NAME": "vmp_product_name",
            "UNIT_OF_MEASURE_IDENTIFIER": "unit_of_measure_identifier",
            "UNIT_OF_MEASURE_NAME": "unit_of_measure_name",
            "TOTAL_QUANITY_IN_VMP_UNIT": "total_quanity_in_vmp_unit",
            "INDICATIVE_COST": "indicative_cost",
        }
        df.rename(columns=column_mapping, inplace=True)

        df["unit_of_measure_name"] = df["unit_of_measure_name"].str.lower()
        df["vmp_snomed_code"] = df["vmp_snomed_code"].astype("int64")
        df["unit_of_measure_identifier"] = df["unit_of_measure_identifier"].astype(
            "int64"
        )
        df["total_quanity_in_vmp_unit"] = df["total_quanity_in_vmp_unit"].astype(float)
        df["indicative_cost"] = df["indicative_cost"].astype(float)

        return self.unit_converter.convert_units(df)

    def update_tables(self, new_status_data: pd.DataFrame) -> None:
        """Updates or creates tables as needed."""
        try:
            existing_data = self._get_existing_data()
            self._update_existing_tables(new_status_data, existing_data)
        except NotFound:
            self._create_new_tables(new_status_data)

    def _create_new_tables(self, status_data: pd.DataFrame) -> None:
        """Creates new tables and imports initial data."""
        self.scmd_manager.create_table(
            description="SCMD dataset",
            partition_field="year_month",
            clustering_fields=["vmp_snomed_code"],
        )
        self.data_status_manager.create_table(
            description="Data status for the SCMD dataset", partition_field="year_month"
        )

        for month in status_data["year_month"]:
            self._update_month_data(month)

    def _update_month_data(self, month: pd.Timestamp) -> None:
        """Updates data for a specific month."""
        month_str = month.strftime("%Y%m%d")
        data = self._process_month_data(month)
        self.scmd_manager.load_dataframe(data, partition_date=month_str)

    def _update_existing_tables(
        self, new_data: pd.DataFrame, existing_data: pd.DataFrame
    ) -> None:
        """Updates existing tables with new data."""
        merged_data = pd.merge(
            new_data,
            existing_data,
            on="year_month",
            how="left",
            suffixes=("_new", "_old"),
        )

        new_months = merged_data[merged_data["file_type_old"].isna()][
            "year_month"
        ].tolist()
        out_of_date_months = merged_data[
            (merged_data["file_type_old"].notna())
            & (merged_data["file_type_new"] != merged_data["file_type_old"])
        ]["year_month"].tolist()

        months_to_update = new_months + out_of_date_months

        logger.info(
            f"Found {len(new_months)} new months and {len(out_of_date_months)} out-of-date months"
        )

        for month in months_to_update:
            self._update_month_data(month)


class Command(BaseCommand):
    """Django management command to import SCMD data into BigQuery."""

    help = "Imports SCMD data into BigQuery, handling both new imports and updates"

    def handle(self, *args, **options):
        """Main entry point for the command."""
        logger.info("Starting SCMD data import process.")

        urls_by_month_and_file_type = {
            f"{dataset_url.month}-01": {
                "url": dataset_url.url,
                "file_type": dataset_url.file_type,
            }
            for dataset_url in DataFetcher.iter_months(DataFetcher.iter_dataset_urls())
        }

        data_status_df = pd.DataFrame(
            [
                (month, details["file_type"])
                for month, details in urls_by_month_and_file_type.items()
            ],
            columns=["year_month", "file_type"],
        ).sort_values(by="year_month")

        data_status_df["year_month"] = pd.to_datetime(data_status_df["year_month"])

        importer = SCMDImporter(urls_by_month_and_file_type)
        importer.update_tables(data_status_df)

        self.stdout.write(self.style.SUCCESS("Successfully imported SCMD data"))
        logger.info("SCMD data import process completed successfully.")
