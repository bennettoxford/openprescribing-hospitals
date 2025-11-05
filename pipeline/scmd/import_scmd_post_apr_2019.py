from prefect import flow, task, get_run_logger

import re
import requests
import pandas as pd
import io
from google.cloud import bigquery
from typing import Dict, List, Iterator
from dataclasses import dataclass

from pipeline.utils.utils import get_bigquery_client
from pipeline.setup.bq_tables import (
    SCMD_RAW_PROVISIONAL_TABLE_SPEC,
    SCMD_RAW_FINALISED_TABLE_SPEC,
)


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

        logger.info(
            f"Found {len(final_urls)} finalised datasets, "
            f"{len(provisional_urls)} provisional datasets"
        )

        return {
            "provisional": provisional_urls,
            "finalised": final_urls,
        }
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
                "TOTAL_QUANITY_IN_VMP_UNIT": "float64",  # Note: CSV has typo
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
def get_existing_dates(table_spec) -> set:
    """
    Gets the existing dates from the specified table.
    Returns empty set if table doesn't exist or has no data.
    """
    client = get_bigquery_client()

    try:
        query = f"""
        SELECT DISTINCT year_month
        FROM {table_spec.full_table_id}
        ORDER BY year_month DESC
        """

        query_job = client.query(query)
        query_result = query_job.result()

        existing_dates = set()
        for row in query_result:
            date_val = row.year_month
            if hasattr(date_val, 'strftime'):
                existing_dates.add(date_val.strftime("%Y-%m-%d"))
            else:
                existing_dates.add(str(date_val))

        return existing_dates
    except Exception as e:
        logger = get_run_logger()
        logger.warning(
            f"Could not fetch existing dates from "
            f"{table_spec.full_table_id}: {e}"
        )
        return set()


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



@task
def get_months_to_update(
    urls_by_month: Dict[str, Dict],
    existing_dates: set,
) -> List[str]:
    """Get the months to update - only new dates not already
    in the table"""
    logger = get_run_logger()

    available_dates = set(urls_by_month.keys())
    new_dates = available_dates - existing_dates

    months_to_update = sorted([date for date in new_dates])

    logger.info(
        f"Found {len(existing_dates)} existing dates, "
        f"{len(available_dates)} available dates, "
        f"{len(new_dates)} new dates to import"
    )

    return months_to_update


@task
def map_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map the columns to the correct names"""
    column_mapping = {
        "YEAR_MONTH": "year_month",
        "ODS_CODE": "ods_code",
        "VMP_SNOMED_CODE": "vmp_snomed_code",
        "VMP_PRODUCT_NAME": "vmp_product_name",
        "UNIT_OF_MEASURE_IDENTIFIER": "unit_of_measure_identifier",
        "UNIT_OF_MEASURE_NAME": "unit_of_measure_name",
        "INDICATIVE_COST": "indicative_cost",
    }
    
    # Handle the typo in the quantity column name
    if "TOTAL_QUANITY_IN_VMP_UNIT" in df.columns:
        column_mapping[
            "TOTAL_QUANITY_IN_VMP_UNIT"
        ] = "total_quantity_in_vmp_unit"
    elif "TOTAL_QUANTITY_IN_VMP_UNIT" in df.columns:
        column_mapping[
            "TOTAL_QUANTITY_IN_VMP_UNIT"
        ] = "total_quantity_in_vmp_unit"

    df.rename(columns=column_mapping, inplace=True)

    if "unit_of_measure_name" in df.columns:
        df["unit_of_measure_name"] = df["unit_of_measure_name"].str.lower()

    return df


@flow(name="SCMD Import Post April 2019")
def import_scmd_post_apr_2019():
    """Main flow for importing SCMD data"""
    logger = get_run_logger()


    urls_by_type = fetch_dataset_urls()
    provisional_urls = urls_by_type["provisional"]
    finalised_urls = urls_by_type["finalised"]

    existing_provisional_dates = get_existing_dates(
        SCMD_RAW_PROVISIONAL_TABLE_SPEC
    )
    existing_finalised_dates = get_existing_dates(
        SCMD_RAW_FINALISED_TABLE_SPEC
    )

    provisional_months_to_update = get_months_to_update(
        provisional_urls, existing_provisional_dates
    )
    finalised_months_to_update = get_months_to_update(
        finalised_urls, existing_finalised_dates
    )

    processed_months = []


    if provisional_months_to_update:
        logger.info(
            f"Processing {len(provisional_months_to_update)} "
            f"months of provisional data"
        )
        for i, month in enumerate(provisional_months_to_update, 1):
            url_data = provisional_urls[month]
            processed_data = process_month_data(month=month, url=url_data["url"])
            processed_data = map_columns(processed_data)

            load_partition(
                df=processed_data,
                partition_date=month,
                table_spec=SCMD_RAW_PROVISIONAL_TABLE_SPEC,
            )

            processed_months.append(
                {
                    "month": month,
                    "file_type": "provisional",
                    "rows": len(processed_data),
                }
            )

            logger.info(
                f"Completed provisional month "
                f"{i}/{len(provisional_months_to_update)}: {month}"
            )
    else:
        logger.info("No new provisional months to import")

    if finalised_months_to_update:
        logger.info(
            f"Processing {len(finalised_months_to_update)} "
            f"months of finalised data"
        )
        for i, month in enumerate(finalised_months_to_update, 1):
            url_data = finalised_urls[month]
            processed_data = process_month_data(month=month, url=url_data["url"])
            processed_data = map_columns(processed_data)

            load_partition(
                df=processed_data,
                partition_date=month,
                table_spec=SCMD_RAW_FINALISED_TABLE_SPEC,
            )

            processed_months.append(
                {
                    "month": month,
                    "file_type": "finalised",
                    "rows": len(processed_data),
                }
            )

            logger.info(
                f"Completed finalised month "
                f"{i}/{len(finalised_months_to_update)}: {month}"
            )
    else:
        logger.info("No new finalised months to import")

    if not processed_months:
        logger.info("No months to update")



if __name__ == "__main__":
    import_scmd_post_apr_2019()
