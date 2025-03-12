from prefect import flow, task, get_run_logger

import re
import requests
import pandas as pd
import io
from google.cloud import bigquery
from typing import Dict, List, Iterator
from dataclasses import dataclass

from utils import get_bigquery_client
from config import PROJECT_ID, DATASET_ID, SCMD_DATA_STATUS_TABLE_ID, SCMD_RAW_TABLE_ID, ORGANISATION_TABLE_ID, UNITS_CONVERSION_TABLE_ID


@dataclass
class DatasetURL:
    month: str
    file_type: str
    url: str


@task(retries=3, retry_delay_seconds=60)
def fetch_dataset_urls() -> Dict[str, Dict]:
    """Fetch all available SCMD dataset URLs"""
    logger = get_run_logger()
    logger.info("Fetching SCMD dataset URLs")
    try:
        def iter_dataset_urls() -> Iterator[str]:
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
                if resource["format"].upper() == "CSV"
                and re.search(pattern, resource["url"].split("/")[-1])
            )

        def iter_months(urls: Iterator[str]) -> Iterator[DatasetURL]:
            pattern = r"scmd_(final|provisional|wip)_([0-9]{4})([0-9]{2})\.csv"
            for url in urls:
                match = re.search(pattern, url.split("/")[-1])
                if match:
                    file_type, year, month = match.groups()
                    yield DatasetURL(month=f"{year}-{month}", file_type=file_type, url=url)
                else:
                    raise ValueError(f"Unexpected URL format: {url}")

        urls_by_month = {
            f"{dataset_url.month}-01": {
                "url": dataset_url.url,
                "file_type": dataset_url.file_type,
            }
            for dataset_url in iter_months(iter_dataset_urls())
        }
        logger.info(f"Found {len(urls_by_month)} datasets")
        return urls_by_month
    except requests.RequestException as e:
        logger.error(f"Failed to fetch dataset URLs: {str(e)}")
        raise

@task
def process_month_data(
    month: str,
    url: str,
    ) -> pd.DataFrame:
    """Process data for a single month"""
    logger = get_run_logger()
    logger.info(f"Processing data for month: {month}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        df = pd.read_csv(io.StringIO(response.text), dtype={
            "YEAR_MONTH": "str",
            "ODS_CODE": "str",
            "VMP_SNOMED_CODE": "str",
            "VMP_PRODUCT_NAME": "str",
            "UNIT_OF_MEASURE_IDENTIFIER": "str",
            "UNIT_OF_MEASURE_NAME": "str",
            "TOTAL_QUANTITY_IN_VMP_UNIT": "float64",
            "INDICATIVE_COST": "float64"
        })
        df["YEAR_MONTH"] = pd.to_datetime(month).date()
        logger.info(f"Successfully processed {len(df)} rows for {month}")
        return df
    except (requests.RequestException, pd.errors.EmptyDataError) as e:
        logger.error(f"Failed to process data for {month}: {str(e)}")
        raise

@task(cache_policy=None)
def get_existing_data_status(client: bigquery.Client, data_status_table_full_id: str) -> pd.DataFrame:
        """
        Gets the existing data from the data status table.
        """
        query = f"""
        SELECT DISTINCT year_month, file_type 
        FROM {data_status_table_full_id}
        ORDER BY year_month DESC
        """
   
        query_job = client.query(query)
        query_result = query_job.result()

        # This is a way around missing read permissions for the project.
        # Update to return query_job.to_dataframe() when permissions are fixed.
        rows = {i.year_month: i.file_type for i in query_result}

        rows_df = pd.DataFrame(rows.items(), columns=["year_month", "file_type"])
        rows_df["year_month"] = pd.to_datetime(rows_df["year_month"], format="%Y-%m-%d")

        return rows_df


@task(cache_policy=None)
def load_partition(
    client: bigquery.Client,
    df: pd.DataFrame,
    partition_date: str,
    table_id: str,
    ) -> None:
    """Loads a DataFrame into the BigQuery table.
    
    Args:
        client: BigQuery client instance
        df: DataFrame containing the data to load
        partition_date: Date string for the partition (YYYY-MM-DD)
        table_id: Full table ID including project and dataset
    
    Raises:
        google.api_core.exceptions.GoogleAPIError: If the load job fails
    """
    partition_date = partition_date.replace("-", "")
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    target_table = (
        f"{table_id}${partition_date}"
    )
    job = client.load_table_from_dataframe(
        df, target_table, job_config=job_config
    )
    job.result()

@task
def get_months_to_update(
    urls_by_month_df: pd.DataFrame,
    existing_data_status: pd.DataFrame,
    ) -> List[str]:
    """Get the months to update"""
    merged_df = pd.merge(
        urls_by_month_df,
        existing_data_status,
        on="year_month",
        how="left",
        suffixes=("_new", "_existing")
    )

    new_months = [d.strftime("%Y-%m-%d") for d in 
                 merged_df[merged_df["file_type_existing"].isna()]["year_month"]]

    out_of_date_months = [d.strftime("%Y-%m-%d") for d in 
                         merged_df[merged_df["file_type_existing"] != merged_df["file_type_new"]]["year_month"]]

    return new_months, out_of_date_months

@task
def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map the columns to the correct names"""
    df.rename(columns={
        "YEAR_MONTH": "year_month",
        "ODS_CODE": "ods_code",
        "VMP_SNOMED_CODE": "vmp_snomed_code",
        "VMP_PRODUCT_NAME": "vmp_product_name",
        "UNIT_OF_MEASURE_IDENTIFIER": "unit_of_measure_identifier",
        "UNIT_OF_MEASURE_NAME": "unit_of_measure_name",
        "TOTAL_QUANITY_IN_VMP_UNIT": "total_quantity_in_vmp_unit",
        "INDICATIVE_COST": "indicative_cost"
    }, inplace=True)
    return df


@flow(name="SCMD Import Pipeline")
def scmd_import():
    """Main flow for importing SCMD data"""
    logger = get_run_logger()

    client = get_bigquery_client()
    scmd_raw_table_id = f"{PROJECT_ID}.{DATASET_ID}.{SCMD_RAW_TABLE_ID}"
    data_status_table_full_id = f"{PROJECT_ID}.{DATASET_ID}.{SCMD_DATA_STATUS_TABLE_ID}"
    existing_data_status = get_existing_data_status(client, data_status_table_full_id)

    urls_by_month = fetch_dataset_urls()

    urls_by_month_df = pd.DataFrame([
        {
            'year_month': pd.to_datetime(key),
            'file_type': value['file_type']
        }
        for key, value in urls_by_month.items()
    ])

    new_months, out_of_date_months = get_months_to_update(urls_by_month_df, existing_data_status)
    logger.info(f"Found {len(new_months)} new months and {len(out_of_date_months)} out of date months")

    months_to_update = new_months + out_of_date_months
    total_months = len(months_to_update[:12])
    
    if total_months == 0:
        logger.info("No months to update")
        return
        
    logger.info(f"Processing {total_months} months of data")
    
    for i, month in enumerate(months_to_update[:12], 1):
        logger.info(f"Processing month {i}/{total_months}: {month}")
        url_data = urls_by_month[month]
        processed_data = process_month_data(
            month=month,
            url=url_data["url"],
        )
        processed_data = map_columns(processed_data)


        load_partition(
            client=client,
            df=processed_data,
            partition_date=month,
            table_id=scmd_raw_table_id,
        )

        load_partition(
            client=client,
            df=pd.DataFrame([{
                "year_month": pd.to_datetime(month).date(),
                "file_type": url_data["file_type"]
            }]),
            partition_date=month,
            table_id=data_status_table_full_id,
        )
        logger.info(f"Completed month {i}/{total_months} ({(i/total_months)*100:.1f}%)")

if __name__ == "__main__":
    scmd_import()
