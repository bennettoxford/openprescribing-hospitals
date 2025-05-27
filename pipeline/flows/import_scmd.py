from prefect import flow, task, get_run_logger

import re
import requests
import pandas as pd
import io
from google.cloud import bigquery
from typing import Dict, List, Iterator
from dataclasses import dataclass

from pipeline.utils.utils import get_bigquery_client
from pipeline.bq_tables import SCMD_RAW_TABLE_SPEC, SCMD_DATA_STATUS_TABLE_SPEC


@dataclass
class DatasetURL:
    month: str
    file_type: str
    url: str


@task()
def fetch_dataset_urls() -> Dict[str, Dict]:
    """Fetch all available SCMD dataset URLs from both provisional and finalised datasets"""
    logger = get_run_logger()
    logger.info("Fetching SCMD dataset URLs")
    try:

        def iter_dataset_urls(dataset_name: str) -> Iterator[str]:
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
                    yield DatasetURL(
                        month=f"{year}-{month}", file_type=file_type, url=url
                    )
                else:
                    raise ValueError(f"Unexpected URL format: {url}")

        provisional_dataset = "secondary-care-medicines-data-indicative-price"
        provisional_urls = {
            f"{dataset_url.month}-01": {
                "url": dataset_url.url,
                "file_type": dataset_url.file_type,
            }
            for dataset_url in iter_months(iter_dataset_urls(provisional_dataset))
        }

        final_dataset = (
            "finalised-secondary-care-medicines-data-scmd-with-indicative-price"
        )
        final_urls = {
            f"{dataset_url.month}-01": {
                "url": dataset_url.url,
                "file_type": dataset_url.file_type,
            }
            for dataset_url in iter_months(iter_dataset_urls(final_dataset))
        }

        overlapping_months = set(provisional_urls.keys()) & set(final_urls.keys())
        if overlapping_months:
            logger.info(
                f"Found {len(overlapping_months)} months with both provisional and final data. "
                f"Using final data for these months: {sorted(overlapping_months)}"
            )

        urls_by_month = provisional_urls.copy()
        urls_by_month.update(final_urls)

        logger.info(
            f"Found {len(urls_by_month)} total datasets "
            f"({len(final_urls)} final, {len(provisional_urls)} provisional)"
        )

        final_months = [
            m for m, data in urls_by_month.items() if data["file_type"] == "final"
        ]
        provisional_months = [
            m for m, data in urls_by_month.items() if data["file_type"] == "provisional"
        ]
        logger.info(
            f"Final breakdown: {len(final_months)} months with final data, "
            f"{len(provisional_months)} months with provisional data"
        )

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

        df = pd.read_csv(
            io.StringIO(response.text),
            dtype={
                "YEAR_MONTH": "str",
                "ODS_CODE": "str",
                "VMP_SNOMED_CODE": "str",
                "VMP_PRODUCT_NAME": "str",
                "UNIT_OF_MEASURE_IDENTIFIER": "str",
                "UNIT_OF_MEASURE_NAME": "str",
                "TOTAL_QUANTITY_IN_VMP_UNIT": "float64",
                "INDICATIVE_COST": "float64",
            },
        )
        df["YEAR_MONTH"] = pd.to_datetime(month).date()
        logger.info(f"Successfully processed {len(df)} rows for {month}")
        return df
    except (requests.RequestException, pd.errors.EmptyDataError) as e:
        logger.error(f"Failed to process data for {month}: {str(e)}")
        raise


@task
def get_existing_data_status() -> pd.DataFrame:
    """
    Gets the existing data from the data status table.
    """
    client = get_bigquery_client()

    query = f"""
    SELECT DISTINCT year_month, file_type 
    FROM {SCMD_DATA_STATUS_TABLE_SPEC.full_table_id}
    ORDER BY year_month DESC
    """

    query_job = client.query(query)
    query_result = query_job.result()

    rows = {i.year_month: i.file_type for i in query_result}
    rows_df = pd.DataFrame(rows.items(), columns=["year_month", "file_type"])
    rows_df["year_month"] = pd.to_datetime(rows_df["year_month"], format="%Y-%m-%d")

    return rows_df


@task
def load_partition(df: pd.DataFrame, partition_date: str, table_spec) -> Dict:
    """Loads a DataFrame into the BigQuery table."""
    client = get_bigquery_client()

    partition_date = partition_date.replace("-", "")
    job_config = bigquery.LoadJobConfig(
        schema=table_spec.schema,
        write_disposition="WRITE_TRUNCATE",
    )

    target_table = f"{table_spec.full_table_id}${partition_date}"
    job = client.load_table_from_dataframe(df, target_table, job_config=job_config)
    job.result()

    return {
        "partition_date": partition_date,
        "rows_loaded": len(df),
        "status": "success",
    }


@task
def get_months_to_update(
    urls_by_month_df: pd.DataFrame,
    existing_data_status: pd.DataFrame,
) -> List[str]:
    """Get the months to update based on the new publishing model"""
    logger = get_run_logger()

    merged_df = pd.merge(
        urls_by_month_df,
        existing_data_status,
        on="year_month",
        how="left",
        suffixes=("_new", "_existing"),
    )

    # Case 1: Brand new months we've never seen before
    new_months = merged_df[merged_df["file_type_existing"].isna()]

    # Case 2: Months where we're updating from provisional to final
    upgrade_months = merged_df[
        (merged_df["file_type_existing"] == "provisional")
        & (merged_df["file_type_new"] == "final")
    ]

    months_to_update = pd.concat([new_months, upgrade_months])
    months_to_update = (
        months_to_update["year_month"].dt.strftime("%Y-%m-%d").unique().tolist()
    )

    logger.info(f"Found {len(new_months)} new months")
    logger.info(
        f"Found {len(upgrade_months)} months updating from provisional to final"
    )
    logger.info(f"Total unique months to update: {len(months_to_update)}")

    return sorted(months_to_update)


@task
def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map the columns to the correct names"""
    df.rename(
        columns={
            "YEAR_MONTH": "year_month",
            "ODS_CODE": "ods_code",
            "VMP_SNOMED_CODE": "vmp_snomed_code",
            "VMP_PRODUCT_NAME": "vmp_product_name",
            "UNIT_OF_MEASURE_IDENTIFIER": "unit_of_measure_identifier",
            "UNIT_OF_MEASURE_NAME": "unit_of_measure_name",
            "TOTAL_QUANITY_IN_VMP_UNIT": "total_quantity_in_vmp_unit",
            "INDICATIVE_COST": "indicative_cost",
        },
        inplace=True,
    )
    return df


@flow(name="SCMD Import Pipeline")
def scmd_import():
    """Main flow for importing SCMD data"""
    logger = get_run_logger()

    existing_data_status = get_existing_data_status()
    urls_by_month = fetch_dataset_urls()

    urls_by_month_df = pd.DataFrame(
        [
            {"year_month": pd.to_datetime(key), "file_type": value["file_type"]}
            for key, value in urls_by_month.items()
        ]
    )

    months_to_update = get_months_to_update(urls_by_month_df, existing_data_status)

    if not months_to_update:
        logger.info("No months to update")
        return {"updated_months": 0, "status": "no_updates_needed"}

    logger.info(f"Processing {len(months_to_update)} months of data")
    processed_months = []

    for i, month in enumerate(months_to_update, 1):
        url_data = urls_by_month[month]
        processed_data = process_month_data(month=month, url=url_data["url"])
        processed_data = map_columns(processed_data)

        raw_result = load_partition(
            df=processed_data,
            partition_date=month,
            table_spec=SCMD_RAW_TABLE_SPEC,
        )

        status_result = load_partition(
            df=pd.DataFrame(
                [
                    {
                        "year_month": pd.to_datetime(month).date(),
                        "file_type": url_data["file_type"],
                    }
                ]
            ),
            partition_date=month,
            table_spec=SCMD_DATA_STATUS_TABLE_SPEC,
        )

        processed_months.append(
            {
                "month": month,
                "file_type": url_data["file_type"],
                "rows": len(processed_data),
            }
        )

        logger.info(f"Completed month {i}/{len(months_to_update)}")

    return {
        "updated_months": len(processed_months),
        "months": processed_months,
        "status": "completed",
    }


if __name__ == "__main__":
    scmd_import()
