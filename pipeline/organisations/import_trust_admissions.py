import io
from datetime import date

import pandas as pd
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefect import flow, get_run_logger, task

from pipeline.setup.bq_tables import (
    TRUST_ADMISSIONS_FINALISED_TABLE_SPEC,
    TRUST_ADMISSIONS_PROVISIONAL_TABLE_SPEC,
)
from pipeline.utils.utils import get_bigquery_client

# Archive file covering 2018/19 to 2023/24
# This is used only for the years for which there are no provider-level M13 files (Jan 2019–Mar 2020)
HES_ARCHIVE_URL = (
    "https://files.digital.nhs.uk/33/675E20/HES%20MAR%20data%20Apr%202018%20to%20Mar%202025.csv"
)
ARCHIVE_PERIOD_START = date(2019, 1, 1)
ARCHIVE_PERIOD_END = date(2020, 3, 1)

# Add a URL when NHS publishes a new month. Old URLs stay valid.
M13_CSV_URLS = {
    2020: "https://files.digital.nhs.uk/74/EE0D77/HES%20MAR%202020-21%20M13.csv",
    2021: "https://files.digital.nhs.uk/BB/EFB024/HES%20MAR%202021-22%20M13.csv",
    2022: "https://files.digital.nhs.uk/9A/849A1B/HES%20MAR%202022-23%20M13.csv",
    2023: "https://files.digital.nhs.uk/1C/739D7F/HES%20MAR%202023-24%20M13.csv",
    2024: "https://files.digital.nhs.uk/49/958B5A/HES%20MAR%202024-25%20M13.csv",
    2025: "https://files.digital.nhs.uk/F1/007ED5/HES%20MAR%202025-26%20M13.csv",
}
PROVISIONAL_CSV_URLS = {
    2026: "https://files.digital.nhs.uk/55/B3792E/HES%20MAR%202026-27%20M6.csv",
}

# NHS sub-national disclosure: counts 1–7 are published as '*'
SUPPRESSED_COUNT = round((1 + 7) / 2)

ADMISSIONS_COLUMNS = {
    "All specialties: Ordinary Elective": "all_specialties_ordinary_elective",
    "All specialties: Daycase Elective": "all_specialties_daycase_elective",
    "All specialties: Elective total": "all_specialties_elective_total",
    "All specialties: Non-Elective": "all_specialties_non_elective",
}

REQUIRED_COLUMNS = {"Organisation breakdown", "Org Code", "Activity Month", *ADMISSIONS_COLUMNS}


def get_existing_periods() -> set[date]:
    table = TRUST_ADMISSIONS_FINALISED_TABLE_SPEC
    client = get_bigquery_client()
    return {
        row.period
        for row in client.query(f"SELECT DISTINCT period FROM `{table.full_table_id}`").result()
    }


def get_latest_finalised_period() -> date | None:
    client = get_bigquery_client()
    table = TRUST_ADMISSIONS_FINALISED_TABLE_SPEC
    try:
        rows = list(
            client.query(f"SELECT MAX(period) AS latest FROM `{table.full_table_id}`").result()
        )
    except NotFound:
        return None
    if not rows or rows[0].latest is None:
        return None
    return rows[0].latest


def period_to_fy_start_year(period: date) -> int:
    return period.year if period.month >= 4 else period.year - 1


def open_fy_start_year(today: date | None = None) -> int:
    today = today or date.today()
    return today.year if today.month >= 4 else today.year - 1


def m13_publications_to_import(periods: set[date] | None = None) -> list[tuple[int, str]]:
    """Stored M13 CSV URLs for FYs not yet loaded in BigQuery."""
    periods = get_existing_periods() if periods is None else periods
    imported_fys = {period_to_fy_start_year(period) for period in periods}
    return [(fy, url) for fy, url in sorted(M13_CSV_URLS.items()) if fy not in imported_fys]


def provisional_publications_to_import(
    latest_finalised: date,
    publications: dict[int, str] | None = None,
    today: date | None = None,
) -> list[tuple[int, str]]:
    """Provisional CSV URLs for months after the latest finalised period."""
    publications = PROVISIONAL_CSV_URLS if publications is None else publications
    start_fy = period_to_fy_start_year(latest_finalised)
    if latest_finalised.month == 3:
        start_fy += 1
    return [
        (fy, publications[fy])
        for fy in range(start_fy, open_fy_start_year(today) + 1)
        if fy in publications
    ]


def filter_after_finalised(df: pd.DataFrame, latest_finalised: date) -> pd.DataFrame:
    return df[df["period"] > latest_finalised].copy()


def _provider_rows(raw: pd.DataFrame) -> pd.DataFrame:
    rows = raw[raw["Organisation breakdown"] == "Provider"].copy()
    rows = rows[rows["Org Code"].notna() & (rows["Org Code"] != "-")]
    rows = rows[rows["Org Code"].astype(str).str.len() == 3]
    return rows


def _load_new_periods_to_bigquery(
    df: pd.DataFrame,
    existing_periods: set[date] | None = None,
    table_spec=TRUST_ADMISSIONS_FINALISED_TABLE_SPEC,
) -> int:
    client = get_bigquery_client()
    existing_periods = get_existing_periods() if existing_periods is None else existing_periods
    new_df = df[~df["period"].isin(existing_periods)]
    if new_df.empty:
        return 0

    job = client.load_table_from_dataframe(
        new_df,
        table_spec.full_table_id,
        job_config=bigquery.LoadJobConfig(
            schema=table_spec.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()
    return job.output_rows or 0


def replace_provisional_table(df: pd.DataFrame) -> int:
    client = get_bigquery_client()
    table_id = TRUST_ADMISSIONS_PROVISIONAL_TABLE_SPEC.full_table_id
    if df.empty:
        client.query(f"DELETE FROM `{table_id}` WHERE TRUE").result()
        return 0
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            schema=TRUST_ADMISSIONS_PROVISIONAL_TABLE_SPEC.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()
    return job.output_rows or 0


def download_csv_text(url: str) -> str:
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    return r.text

def parse_activity_month(value: str) -> pd.Timestamp:
    value = str(value).strip()
    for fmt in ("%b %Y", "%b-%y"):
        parsed = pd.to_datetime(value, format=fmt, errors="coerce")
        if pd.notna(parsed):
            return parsed
    raise ValueError(f"Unrecognised activity month: {value!r}")

def parse_activity_csv(text: str) -> pd.DataFrame:
    raw = pd.read_csv(io.StringIO(text), low_memory=False)
    missing = REQUIRED_COLUMNS - set(raw.columns)
    if missing:
        raise ValueError(f"CSV missing expected columns: {sorted(missing)}")

    rows = _provider_rows(raw)

    out = pd.DataFrame(
        {
            "ods_code": rows["Org Code"].astype(str).str.strip().str.upper(),
            "period": rows["Activity Month"].map(parse_activity_month).dt.date,
        }
    )
    for src, dst in ADMISSIONS_COLUMNS.items():
        counts = rows[src].replace("*", SUPPRESSED_COUNT)
        out[dst] = pd.to_numeric(counts, errors="coerce").fillna(0).astype(int)
    out["all_specialties_total"] = (
        out["all_specialties_elective_total"] + out["all_specialties_non_elective"]
    )
    return out


def _load_finalised_file(fy: int, csv_url: str) -> int:
    """Append the new (not-yet-loaded) months from one finalised MAR CSV."""
    logger = get_run_logger()
    df = parse_activity_csv(download_csv_text(csv_url))
    if df.empty:
        logger.warning(f"No rows in finalised file for FY {fy}")
        return 0
    existing = get_existing_periods()
    rows = _load_new_periods_to_bigquery(df, existing_periods=existing)
    if rows == 0:
        logger.info(f"Finalised FY {fy}: all months already loaded")
        return 0
    new_months = df[~df["period"].isin(existing)]["period"].nunique()
    logger.info(f"Imported finalised FY {fy} ({new_months} months, {rows} rows)")
    return rows


# --- Step 1: archive backfill (Jan 2019–Mar 2020) ---------------------------
@task
def import_archive() -> int:
    """Backfill the early months that have no per-trust M13 file from the multi-year archive."""
    logger = get_run_logger()
    df = parse_activity_csv(download_csv_text(HES_ARCHIVE_URL))
    df = df[(df["period"] >= ARCHIVE_PERIOD_START) & (df["period"] <= ARCHIVE_PERIOD_END)]
    rows = _load_new_periods_to_bigquery(df)
    logger.info(f"Loaded {rows} rows ({df['period'].nunique()} months in archive window)")
    return rows


# --- Step 2: finalised year-end (M13) files ---------------------------------
@task
def import_finalised_m13() -> int:
    """Append finalised M13 months for each FY not yet complete in BigQuery."""
    logger = get_run_logger()
    publications = m13_publications_to_import()
    if not publications:
        logger.info("M13: no incomplete years in the stored file list")
        return 0

    logger.info(f"M13 files to import: {publications}")
    total = 0
    for fy, csv_url in publications:
        total += _load_finalised_file(fy, csv_url)
    return total


# --- Step 3: provisional months after the latest finalised period -----------
@task
def import_provisional(latest_finalised: date | None = None) -> int:
    """Replace the provisional table with months published after the latest finalised period."""
    logger = get_run_logger()
    latest_finalised = latest_finalised or get_latest_finalised_period()
    if latest_finalised is None:
        logger.warning("No finalised data in BigQuery; skipping provisional import")
        return 0

    pubs = provisional_publications_to_import(latest_finalised)
    if not pubs:
        logger.info(f"No provisional publications found after {latest_finalised}")
        return replace_provisional_table(pd.DataFrame())

    logger.info(f"Provisional files to import: {pubs}")
    frames = [parse_activity_csv(download_csv_text(csv_url)) for _, csv_url in pubs]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    df = filter_after_finalised(df, latest_finalised)
    rows = replace_provisional_table(df)
    months = df["period"].nunique() if not df.empty else 0
    logger.info(
        f"Replaced provisional table with {rows} rows ({months} months after {latest_finalised})"
    )
    return rows


@flow(name="Import HES Trust Admissions")
def import_trust_admissions():
    import_archive()                                    # 1. Jan 2019–Mar 2020 backfill
    import_finalised_m13()                              # 2. finalised year-end (M13) files
    import_provisional(get_latest_finalised_period())   # 3. provisional months after finalised


if __name__ == "__main__":
    import_trust_admissions()
