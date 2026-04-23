import pandas as pd
from prefect import flow, get_run_logger, task
from google.cloud import bigquery

from pipeline.setup.config import PROJECT_ID, DATASET_ID, SHELFORD_GROUP_TRUSTS_TABLE_ID
from pipeline.setup.bq_tables import SHELFORD_GROUP_TRUSTS_TABLE_SPEC
from pipeline.utils.utils import get_bigquery_client

SHELFORD_GROUP_ODS_CODES = (
    "RGT",
    "RP4",
    "RYJ",
    "RJZ",
    "R0A",
    "RTH",
    "RHQ",
    "RTD",
    "RRV",
    "RRK",
)


@task
def write_shelford_group_trusts_to_bq() -> None:
    logger = get_run_logger()
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{SHELFORD_GROUP_TRUSTS_TABLE_ID}"
    df = pd.DataFrame({"ods_code": list(SHELFORD_GROUP_ODS_CODES)})
    job_config = bigquery.LoadJobConfig(
        schema=SHELFORD_GROUP_TRUSTS_TABLE_SPEC.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    logger.info("Wrote %s rows to %s", len(df), table_id)


@flow(name="seed_shelford_group_trusts")
def import_shelford_group_trusts():
    write_shelford_group_trusts_to_bq()


if __name__ == "__main__":
    import_shelford_group_trusts()
