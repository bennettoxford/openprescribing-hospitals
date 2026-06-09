from collections import defaultdict
from datetime import date

from django.db import transaction
from prefect import flow, get_run_logger, task

from pipeline.setup.bq_tables import (
    TRUST_ADMISSIONS_FINALISED_TABLE_SPEC,
    TRUST_ADMISSIONS_PROVISIONAL_TABLE_SPEC,
)
from pipeline.utils.utils import execute_bigquery_query, setup_django_environment

setup_django_environment()
from viewer.models import Organisation, TrustAdmission


@task()
def extract_trust_admissions() -> list[dict]:
    """
    Extract monthly finished spell totals per trust, preferring finalised over provisional.

    For each trust-month, finalised M13 data is used when present; otherwise provisional
    cumulative MAR data is used.
    """
    query = f"""
    WITH finalised AS (
        SELECT
            ods_code,
            period,
            all_specialties_total AS count
        FROM `{TRUST_ADMISSIONS_FINALISED_TABLE_SPEC.full_table_id}`
        WHERE ods_code IS NOT NULL
    ),
    provisional AS (
        SELECT
            ods_code,
            period,
            all_specialties_total AS count
        FROM `{TRUST_ADMISSIONS_PROVISIONAL_TABLE_SPEC.full_table_id}`
        WHERE ods_code IS NOT NULL
    )
    SELECT ods_code, period, count
    FROM finalised
    UNION ALL
    SELECT p.ods_code, p.period, p.count
    FROM provisional p
    LEFT JOIN finalised f
        ON f.ods_code = p.ods_code AND f.period = p.period
    WHERE f.ods_code IS NULL
    ORDER BY ods_code, period
    """
    return execute_bigquery_query(query)


@task()
def load_trust_admissions_data(rows: list[dict]) -> int:
    """Load trust admissions, merged onto successor organisations where applicable."""
    logger = get_run_logger()

    orgs = Organisation.objects.select_related("successor").all()
    org_by_code = {org.ods_code: org for org in orgs}

    def effective_org(org: Organisation) -> Organisation:
        return org.successor if org.successor_id else org

    # (org_id, period) -> total count
    counts: dict[tuple[int, date], int] = defaultdict(int)
    skipped = 0

    for row in rows:
        org = org_by_code.get(row["ods_code"])
        if not org:
            skipped += 1
            continue
        target = effective_org(org)
        period = row["period"]
        counts[(target.id, period)] += int(row["count"] or 0)

    if skipped:
        logger.info(f"Skipped {skipped} admission rows with unknown ODS codes")

    records = [
        TrustAdmission(organisation_id=org_id, period=period, count=count)
        for (org_id, period), count in counts.items()
    ]

    with transaction.atomic():
        TrustAdmission.objects.all().delete()
        TrustAdmission.objects.bulk_create(records, batch_size=1000)

    logger.info(f"Loaded {len(records)} trust admission rows")
    return len(records)


@flow(name="Load Trust Admissions")
def load_trust_admissions():
    rows = extract_trust_admissions()
    load_trust_admissions_data(rows)


if __name__ == "__main__":
    load_trust_admissions()
