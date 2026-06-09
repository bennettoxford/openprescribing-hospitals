import io
import re
from datetime import date
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
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

HES_INDEX_URL = (
    "https://digital.nhs.uk/data-and-information/publications/"
    "statistical/provisional-monthly-hospital-episode-statistics-for-admitted-"
    "patient-care-outpatient-and-accident-and-emergency-data"
)
M13_FIRST_FY = 2020
M13_SLUG = re.compile(r"^april-(?P<fy>\d{4})---march-(?P<march>\d{4})(?:-m13|-month-13)$", re.I)
PROVISIONAL_SLUG = re.compile(
    r"^april-(?P<fy>\d{4})---(?P<month>[a-z]+)-(?P<year>\d{4})$", re.I
)
MONTH_NUM = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
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


def fy_april_start(fy: int) -> date:
    return date(fy, 4, 1)


def fy_march_end(fy: int) -> date:
    return date(fy + 1, 3, 1)


def open_fy_start_year(today: date | None = None) -> int:
    today = today or date.today()
    return today.year if today.month >= 4 else today.year - 1


def parse_provisional_slug(slug: str) -> tuple[int, date] | None:
    match = PROVISIONAL_SLUG.match(slug.lower())
    if not match:
        return None
    fy = int(match.group("fy"))
    month = MONTH_NUM.get(match.group("month").lower())
    if month is None:
        return None
    coverage_end = date(int(match.group("year")), month, 1)
    if coverage_end < fy_april_start(fy) or coverage_end > fy_march_end(fy):
        return None
    return fy, coverage_end


def find_m13_publications() -> dict[int, str]:
    """Find M13 FY publication page URLs keyed by FY start year."""
    r = requests.get(HES_INDEX_URL, timeout=60)
    r.raise_for_status()

    base = HES_INDEX_URL.rstrip("/")
    discovered: dict[int, str] = {}
    seen_slugs: set[str] = set()
    for link in BeautifulSoup(r.text, "html.parser").find_all("a", href=True):
        href = link["href"]
        url = href if href.startswith("http") else urljoin("https://digital.nhs.uk", href)
        if not url.startswith(base):
            continue

        slug = url.rstrip("/").split("/")[-1].lower()
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)

        match = M13_SLUG.match(slug)
        if not match:
            continue

        fy = int(match.group("fy"))
        if fy >= M13_FIRST_FY and int(match.group("march")) == fy + 1:
            discovered[fy] = url
    return dict(sorted(discovered.items()))


def m13_publications_to_import(periods: set[date] | None = None) -> list[tuple[int, str]]:
    """Discovered M13 publication pages for FYs not yet loaded in BigQuery."""
    periods = get_existing_periods() if periods is None else periods
    discovered = find_m13_publications()
    imported_fys = {period_to_fy_start_year(period) for period in periods}
    return [(fy, url) for fy, url in discovered.items() if fy not in imported_fys]


def discover_provisional_publications(html: str | None = None) -> dict[int, str]:
    """Latest provisional publication page URL per FY (most recent cumulative release)."""
    if html is None:
        response = requests.get(HES_INDEX_URL, timeout=60)
        response.raise_for_status()
        html = response.text

    base = HES_INDEX_URL.rstrip("/")
    best: dict[int, tuple[date, str]] = {}
    seen_slugs: set[str] = set()
    for link in BeautifulSoup(html, "html.parser").find_all("a", href=True):
        href = link["href"]
        url = href if href.startswith("http") else urljoin("https://digital.nhs.uk", href)
        if not url.startswith(base):
            continue

        slug = url.rstrip("/").split("/")[-1].lower()
        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)

        parsed = parse_provisional_slug(slug)
        if not parsed:
            continue

        fy, coverage_end = parsed
        if fy not in best or coverage_end > best[fy][0]:
            best[fy] = (coverage_end, url)
    return {fy: url for fy, (_, url) in sorted(best.items())}


def provisional_publications_to_import(
    latest_finalised: date,
    html: str | None = None,
    today: date | None = None,
) -> list[tuple[int, str]]:
    """Latest provisional page per FY from the latest finalised FY through the open FY."""
    discovered = discover_provisional_publications(html)
    start_fy = period_to_fy_start_year(latest_finalised)
    return [
        (fy, discovered[fy])
        for fy in range(start_fy, open_fy_start_year(today) + 1)
        if fy in discovered
    ]


def filter_after_finalised(df: pd.DataFrame, latest_finalised: date) -> pd.DataFrame:
    return df[df["period"] > latest_finalised].copy()


def resolve_m13_csv_links(publications: list[tuple[int, str]]) -> list[tuple[int, str, str]]:
    """Resolve one MAR Open Data CSV link per M13 publication (FY, page_url, csv_url)."""
    resolved: list[tuple[int, str, str]] = []
    for fy, page_url in publications:
        response = requests.get(page_url, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        for container in soup.find_all(["article", "li", "section", "div"]):
            container_text = " ".join(container.get_text(" ", strip=True).lower().split())
            if (
                "monthly activity report" not in container_text
                or "open data" not in container_text
                or "open data -" in container_text
            ):
                continue

            for link in container.find_all("a", href=True):
                href = link["href"]
                url = href if href.startswith("http") else urljoin("https://digital.nhs.uk", href)
                low = url.lower()
                if "files.digital.nhs.uk" in low and low.endswith(".csv"):
                    resolved.append((fy, page_url, url))
                    break
            else:
                continue
            break
    return resolved


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
        logger.info("M13: no incomplete discovered M13 years found on the index")
        return 0

    logger.info(f"M13 publications to import (discovered on index): {publications}")
    total = 0
    for fy, _page_url, csv_url in resolve_m13_csv_links(publications):
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

    logger.info(f"Provisional publications to import: {pubs}")
    csv_links = resolve_m13_csv_links(pubs)
    frames = [parse_activity_csv(download_csv_text(csv_url)) for _, _, csv_url in csv_links]
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
