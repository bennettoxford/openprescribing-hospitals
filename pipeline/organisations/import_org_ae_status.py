import requests
import re
import io
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from prefect import task, flow, get_run_logger
from google.cloud import bigquery

from pipeline.utils.utils import get_bigquery_client
from pipeline.setup.bq_tables import ORG_AE_STATUS_TABLE_SPEC


@task()
def fetch_ae_data() -> pd.DataFrame:
    """Fetch A&E attendance data from NHS England website."""
    logger = get_run_logger()
    base_url = "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/"

    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, "html.parser")

    links = soup.find_all(
        "a",
        href=lambda href: href and "ae-attendances-and-emergency-admissions" in href,
    )
    all_data = []

    total_links = len(links)
    logger.info(f"Found {total_links} links of data to process")

    for year_index, link in enumerate(links, 1):
        year_url = link["href"]
        year_response = requests.get(year_url)
        year_soup = BeautifulSoup(year_response.content, "html.parser")

        csv_links = year_soup.find_all(
            "a",
            href=lambda href: href and href.endswith(".csv") and "Monthly-AE" in href,
        )
        total_csvs = len(csv_links)

        for csv_index, csv_link in enumerate(csv_links, 1):
            file_url = urljoin(year_url, csv_link["href"])
            file_response = requests.get(file_url)

            pattern = r"Monthly-AE-(\w+)-(\d{4})"
            year = re.search(pattern, file_url).group(1)
            month = re.search(pattern, file_url).group(2)
            year_month = pd.to_datetime(f"{year}-{month}")

            columns = (
                ["Org Code", "A&E attendances Type 1"]
                if year_month >= pd.to_datetime("2020-09")
                else ["Org Code", "Number of A&E attendances Type 1"]
            )

            if year_month == pd.to_datetime("2020-08"):
                df = pd.read_csv(
                    io.StringIO(file_response.text),
                    usecols=[0, 1, 4],
                    names=columns,
                )
            else:
                df = pd.read_csv(
                    io.StringIO(file_response.text),
                    usecols=columns,
                )
                df = df.drop(df.index[-1])

            df["Period"] = year_month
            all_data.append(df)

            if csv_index == 1 or csv_index % 6 == 0 or csv_index == total_csvs:
                logger.info(
                    f"Year {year_index}/{total_links}: Processed {csv_index}/{total_csvs} months"
                )

    combined_data = pd.concat(all_data, ignore_index=True)
    combined_data["has_ae"] = combined_data["A&E attendances Type 1"] > 0
    combined_data = combined_data[["Period", "Org Code", "has_ae"]]
    combined_data = combined_data.rename(
        columns={"Org Code": "ods_code", "Period": "period"}
    )

    logger.info(f"Completed processing with {len(combined_data)} total records")
    return combined_data


@task
def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare dataframe with correct data types."""
    df["period"] = df["period"].dt.date

    dtypes = {
        "ods_code": str,
        "has_ae": bool,
    }
    return df.astype(dtypes)


@task
def upload_to_bigquery(df: pd.DataFrame) -> None:
    """Upload dataframe to BigQuery."""
    logger = get_run_logger()
    client = get_bigquery_client()

    job_config = bigquery.LoadJobConfig(
        schema=ORG_AE_STATUS_TABLE_SPEC.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(
        df, ORG_AE_STATUS_TABLE_SPEC.full_table_id, job_config=job_config
    )
    job.result()

    logger.info(
        f"Loaded {job.output_rows} rows into {ORG_AE_STATUS_TABLE_SPEC.full_table_id}"
    )


@flow(name="Import AE Status")
def import_org_ae_status():
    """Main flow to import A&E status data."""
    df = fetch_ae_data()
    df = prepare_dataframe(df)
    upload_to_bigquery(df)


if __name__ == "__main__":
    import_org_ae_status()
