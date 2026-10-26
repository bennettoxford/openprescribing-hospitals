import hashlib
import zipfile
import xml.etree.ElementTree as ET

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger

from pipeline.dmd.extracts import (
    XML_EXTRACTS,
    extracts_by_prefix,
    map_row,
    matching_extract,
    raw_specs,
)
from pipeline.setup.bq_tables import DMD_UOM_TABLE_SPEC
from pipeline.setup.config import DATASET_ID, PROJECT_ID
from pipeline.utils.utils import get_bigquery_client, validate_table_schema

DMD_TRUD_ITEM_ID = "24"
DATA_DIR = Path("data/dmd")
RAW_SPECS = raw_specs(PROJECT_ID, DATASET_ID)


def local_tag(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def child_values(elem: ET.Element) -> dict[str, str]:
    values = {}
    for child in elem:
        text = (child.text or "").strip()
        if not text:
            continue
        values[local_tag(child.tag)] = text
    return values


def iter_xml_records(source) -> Iterator[tuple[str | None, str, dict[str, str]]]:
    """Yield the parent tag, the record tag, and the direct child text values."""
    stack = []
    for event, elem in ET.iterparse(source, events=("start", "end")):
        if event == "start":
            stack.append(elem)
            continue
        stack.pop()
        tag = local_tag(elem.tag)
        values = child_values(elem)
        if not values:
            continue
        parent = local_tag(stack[-1].tag) if stack else None
        yield parent, tag, values
        elem.clear()
        if stack:
            stack[-1].remove(elem)


def xml_member(names: list[str], prefix: str) -> str:
    matches = [
        name
        for name in names
        if Path(name).name.startswith(prefix) and name.endswith(".xml")
    ]
    if len(matches) != 1:
        raise FileNotFoundError(f"Expected one {prefix} XML file, found {matches}")
    return matches[0]


def parse_dmd_release_zip(zip_path: Path) -> dict[str, list[dict]]:
    rows = {extract.key: [] for extract in XML_EXTRACTS}
    with zipfile.ZipFile(zip_path) as archive:
        names = archive.namelist()
        for prefix, extracts in extracts_by_prefix().items():
            member = xml_member(names, prefix)
            with archive.open(member) as source:
                for parent, tag, values in iter_xml_records(source):
                    extract = matching_extract(extracts, parent, tag)
                    if extract is None:
                        continue
                    rows[extract.key].append(map_row(extract, values))

    empty = [key for key, records in rows.items() if not records]
    if empty:
        raise ValueError(f"No rows parsed for {', '.join(empty)}")
    return rows


def select_current_uom_rows(unit_rows: list[dict]) -> list[dict]:
    """Drop a unit code when another row lists it as the previous code."""
    previous_codes = {
        row["uom_code_prev"] for row in unit_rows if row["uom_code_prev"]
    }
    current_rows = [row for row in unit_rows if row["uom_code"] not in previous_codes]
    if not current_rows:
        raise ValueError("No current unit of measure rows after removing replaced codes")
    return current_rows


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True)
class DmdRelease:
    release_date: str
    download_url: str
    archive_file_name: str
    sha256: str
    size_bytes: int


def fetch_latest_dmd_release(api_key: str) -> DmdRelease:
    url = (
        "https://isd.digital.nhs.uk/trud/api/v1/keys/"
        f"{api_key}/items/{DMD_TRUD_ITEM_ID}/releases?latest"
    )
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    release = response.json()["releases"][0]
    return DmdRelease(
        release_date=release["releaseDate"],
        download_url=release["archiveFileUrl"],
        archive_file_name=release["archiveFileName"],
        sha256=release["archiveFileSha256"].lower(),
        size_bytes=release["archiveFileSizeBytes"],
    )


def download_dmd_archive(release: DmdRelease) -> Path:
    dest_dir = DATA_DIR / release.release_date.replace("-", "_")
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / release.archive_file_name
    if (
        dest.exists()
        and dest.stat().st_size == release.size_bytes
        and file_sha256(dest) == release.sha256
    ):
        return dest

    with requests.get(release.download_url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with dest.open("wb") as target:
            for block in response.iter_content(32 * 1024):
                if block:
                    target.write(block)

    if file_sha256(dest) != release.sha256:
        raise ValueError("Downloaded dm+d archive checksum does not match TRUD")
    return dest


@task
def get_latest_dmd_release() -> DmdRelease:
    api_key = Secret.load("trud-api-key").get()
    return fetch_latest_dmd_release(api_key)


@task
def loaded_release_sha() -> str | None:
    client = get_bigquery_client()
    table_id = RAW_SPECS["release"].full_table_id
    try:
        client.get_table(table_id)
    except NotFound:
        return None
    query = f"SELECT sha256 FROM `{table_id}` LIMIT 1"
    rows = list(client.query(query).result())
    if not rows:
        return None
    return rows[0].sha256


def load_table(client: bigquery.Client, table_spec, table_rows: list[dict]) -> None:
    logger = get_run_logger()
    logger.info(f"Loading {len(table_rows)} rows into {table_spec.full_table_id}")
    job_config = bigquery.LoadJobConfig(
        schema=table_spec.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_json(
        table_rows, table_spec.full_table_id, job_config=job_config
    )
    job.result()


@task
def load_raw_tables(rows: dict[str, list[dict]]) -> None:
    client = get_bigquery_client()
    for extract in XML_EXTRACTS:
        load_table(client, RAW_SPECS[extract.key], rows[extract.key])
    load_table(
        client,
        DMD_UOM_TABLE_SPEC,
        select_current_uom_rows(rows["unit_of_measure"]),
    )


@task
def validate_uom_data():
    logger = get_run_logger()
    client = get_bigquery_client()
    table_id = DMD_UOM_TABLE_SPEC.full_table_id

    duplicate_codes_query = f"""
    WITH all_codes AS (
        SELECT uom_code AS code FROM `{table_id}`
        UNION ALL
        SELECT uom_code_prev AS code
        FROM `{table_id}`
        WHERE uom_code_prev IS NOT NULL
    )
    SELECT code, COUNT(*) AS occurrences
    FROM all_codes
    WHERE code IS NOT NULL
    GROUP BY code
    HAVING COUNT(*) > 1
    """
    duplicate_codes = [dict(row) for row in client.query(duplicate_codes_query).result()]

    if duplicate_codes:
        logger.error(f"Found {len(duplicate_codes)} codes that appear multiple times")
        for code in duplicate_codes[:5]:
            logger.error(f"- Code {code['code']} appears {code['occurrences']} times")
        raise ValueError("UOM codes must be unique")

    if not validate_table_schema(DMD_UOM_TABLE_SPEC):
        raise ValueError(
            f"Schema validation failed for {DMD_UOM_TABLE_SPEC.full_table_id}"
        )

    logger.info("UOM data validated")


@task
def record_dmd_release(release: DmdRelease) -> None:
    row = {
        "archive_file_name": release.archive_file_name,
        "release_date": release.release_date,
        "sha256": release.sha256,
        "imported_at": datetime.now(timezone.utc).isoformat(),
    }
    load_table(get_bigquery_client(), RAW_SPECS["release"], [row])


@flow(name="Import dm+d raw")
def import_dmd_raw():
    """Download the latest TRUD dm+d release and load the raw tables."""
    logger = get_run_logger()
    logger.info("Starting raw dm+d import")

    release = get_latest_dmd_release()
    if loaded_release_sha() == release.sha256:
        logger.info(f"dm+d release {release.archive_file_name} is already loaded")
        return

    logger.info(f"Loading dm+d release {release.archive_file_name}")
    zip_path = download_dmd_archive(release)
    rows = parse_dmd_release_zip(zip_path)
    load_raw_tables(rows)
    validate_uom_data()
    record_dmd_release(release)
    logger.info(f"Loaded dm+d release {release.archive_file_name}")


if __name__ == "__main__":
    import_dmd_raw()
