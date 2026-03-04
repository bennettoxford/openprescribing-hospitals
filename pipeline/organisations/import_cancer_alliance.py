import io
from typing import Dict, Optional

import pandas as pd
from google.cloud import bigquery, storage
from prefect import flow, get_run_logger, task

from pipeline.setup.config import (
    BUCKET_NAME,
    PROJECT_ID,
    DATASET_ID,
    ORGANISATION_TABLE_ID,
    CANCER_ALLIANCE_CATEGORISATIONS_TABLE_ID,
)

ODS_CA_MAPPING_GCS_PATH = "oph/ods_ca_mapping.csv"
from pipeline.setup.bq_tables import CANCER_ALLIANCE_CATEGORISATIONS_TABLE_SPEC
from pipeline.utils.utils import get_bigquery_client


@task
def fetch_organisations_from_bq() -> pd.DataFrame:
    logger = get_run_logger()
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{ORGANISATION_TABLE_ID}"

    query = f"""
    SELECT ods_code, ods_name, postcode, region_code, region, icb_code, icb,
           successors, predecessors, ultimate_successors,
           legal_closed_date, operational_closed_date, legal_open_date, operational_open_date
    FROM `{table_id}`
    """
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} organisations from BigQuery")
    return df


@task
def fetch_ods_ca_mapping_from_gcs() -> Dict[str, Dict[str, Optional[str]]]:
    """Fetch ODS-to-Cancer-Alliance mapping from CSV on GCS. Returns ods_code -> {code, name, notes}."""
    logger = get_run_logger()
    bq_client = get_bigquery_client()
    storage_client = storage.Client(
        project=bq_client.project,
        credentials=bq_client._credentials,
    )
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(ODS_CA_MAPPING_GCS_PATH)
    content = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(content))
    # CSV columns: ods_code, ods_name, cancer_alliance_name, cancer_alliance_code, notes
    result = {}
    for _, row in df.iterrows():
        ods = row.get("ods_code")
        code = row.get("cancer_alliance_code")
        name = row.get("cancer_alliance_name")
        notes = row.get("notes")
        if pd.notna(ods) and pd.notna(code) and pd.notna(name):
            ods_str = str(ods).strip()
            notes_val = str(notes).strip() if pd.notna(notes) and str(notes).strip() else None
            result[ods_str] = {
                "cancer_alliance_code": str(code).strip(),
                "cancer_alliance_name": str(name).strip(),
                "notes": notes_val,
            }
    logger.info(f"Loaded {len(result)} ODS-Cancer Alliance mappings from GCS")
    return result


@task
def enrich_organisations_with_cancer_alliance(
    df: pd.DataFrame,
    ods_ca_mapping: Dict[str, Dict[str, Optional[str]]],
) -> pd.DataFrame:
    """Add cancer_alliance_code, cancer_alliance and notes to each org using the mapping."""
    logger = get_run_logger()
    codes = []
    names = []
    notes_list = []
    for _, row in df.iterrows():
        ods = row.get("ods_code")
        ca = ods_ca_mapping.get(str(ods).strip()) if ods else None
        if ca:
            codes.append(ca["cancer_alliance_code"])
            names.append(ca["cancer_alliance_name"])
            notes_list.append(ca["notes"])
        else:
            codes.append(None)
            names.append(None)
            notes_list.append(None)
    df = df.copy()
    df["cancer_alliance_code"] = codes
    df["cancer_alliance"] = names
    df["notes"] = notes_list
    matched = sum(1 for c in codes if c is not None)
    logger.info(f"Matched {matched}/{len(df)} organisations to Cancer Alliances")
    return df


@task
def write_cancer_alliance_categorisations_to_bq(df: pd.DataFrame) -> None:
    logger = get_run_logger()
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{CANCER_ALLIANCE_CATEGORISATIONS_TABLE_ID}"

    categorisations_df = df[
        ["ods_code", "cancer_alliance_code", "cancer_alliance", "notes"]
    ].copy()
    categorisations_df = categorisations_df.rename(
        columns={"cancer_alliance": "cancer_alliance_name"}
    )

    job_config = bigquery.LoadJobConfig(
        schema=CANCER_ALLIANCE_CATEGORISATIONS_TABLE_SPEC.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(
        categorisations_df, table_id, job_config=job_config
    )
    job.result()
    logger.info(f"Wrote {len(categorisations_df)} cancer alliance categorisations to {table_id}")


@flow(name="Populate Cancer Alliance")
def import_cancer_alliance_data():
    logger = get_run_logger()
    logger.info("Starting Cancer Alliance population")

    df = fetch_organisations_from_bq()
    if df.empty:
        logger.info("No organisations to process")
        return

    ods_ca_mapping = fetch_ods_ca_mapping_from_gcs()
    df = enrich_organisations_with_cancer_alliance(df, ods_ca_mapping)
    write_cancer_alliance_categorisations_to_bq(df)

    logger.info("Cancer Alliance population complete")


if __name__ == "__main__":
    import_cancer_alliance_data()
