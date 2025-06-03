from prefect import task, flow, get_run_logger
from django.db import transaction
from typing import List, Dict
from pipeline.utils.config import (
    PROJECT_ID,
    DATASET_ID,
    ORGANISATION_TABLE_ID,
    SCMD_PROCESSED_TABLE_ID,
)
from pipeline.utils.utils import setup_django_environment, execute_bigquery_query

setup_django_environment()
from viewer.models import Organisation


@task()
def extract_organisations() -> List[Dict]:
    """Extract organisation data from BigQuery, filtering for only those in SCMD processed data"""
    logger = get_run_logger()
    logger.info("Starting extraction of organisation data from BigQuery")

    query = f"""
    SELECT DISTINCT
        org.ods_code, 
        org.ods_name, 
        org.region, 
        org.icb, 
        org.successors, 
        org.ultimate_successors
    FROM 
        `{PROJECT_ID}.{DATASET_ID}.{ORGANISATION_TABLE_ID}` org
    WHERE 
        org.ods_code IN (
            SELECT DISTINCT ods_code 
            FROM `{PROJECT_ID}.{DATASET_ID}.{SCMD_PROCESSED_TABLE_ID}`
        )
    """

    data = execute_bigquery_query(query)
    logger.info(f"Extracted {len(data)} organisation records from BigQuery")
    return data


@task
def transform_organisations(data: List[Dict]) -> List[Dict]:
    """Transform the data to match Django model structure"""
    logger = get_run_logger()
    logger.info("Transforming organisation data")

    successor_map = {}
    for row in data:
        if row.get("ultimate_successors") and len(row["ultimate_successors"]) > 0:
            successor_map[row["ods_code"]] = row["ultimate_successors"][-1]

    transformed_data = []
    for row in data:
        transformed_row = {
            "ods_code": row["ods_code"],
            "ods_name": row["ods_name"],
            "region": row["region"] or "",
            "icb": row["icb"],
            "successor_code": successor_map.get(row["ods_code"]),
        }
        transformed_data.append(transformed_row)

    logger.info(f"Transformed {len(transformed_data)} organisation records")
    return transformed_data


@task
def load_organisations(data: List[Dict]) -> Dict:
    """Load organization data by replacing all existing data"""
    logger = get_run_logger()
    logger.info(f"Loading {len(data)} organisation records")

    with transaction.atomic():

        deleted_count = Organisation.objects.all().delete()[0]
        logger.info(f"Deleted {deleted_count} existing organisation records")

        organisation_objects = [
            Organisation(
                ods_code=row["ods_code"],
                ods_name=row["ods_name"],
                region=row["region"],
                icb=row["icb"],
            )
            for row in data
        ]

        created_objects = Organisation.objects.bulk_create(
            organisation_objects, batch_size=1000
        )

        total_created = len(created_objects)
        logger.info(f"Created {total_created} organisation records")

        successor_updates = 0

        org_lookup = {org.ods_code: org for org in Organisation.objects.all()}

        orgs_to_update = []
        for row in data:
            if row.get("successor_code") and row["successor_code"] in org_lookup:
                org = org_lookup[row["ods_code"]]
                successor = org_lookup[row["successor_code"]]
                org.successor = successor
                orgs_to_update.append(org)
                successor_updates += 1

        if orgs_to_update:
            Organisation.objects.bulk_update(
                orgs_to_update, ["successor"], batch_size=1000
            )
            logger.info(f"Updated {successor_updates} successor relationships")

    logger.info(
        f"Organisation data load complete. Deleted: {deleted_count}, Created: {total_created}, Updated successors: {successor_updates}"
    )
    return {
        "deleted": deleted_count,
        "created": total_created,
        "updated_successors": successor_updates,
        "total_records": len(data),
    }


@flow(name="Load Organisations")
def load_organisations_flow():
    """Main flow to import organisation data from BigQuery to Django"""
    logger = get_run_logger()
    logger.info("Starting organisation import flow")

    org_data = extract_organisations()
    transformed_data = transform_organisations(org_data)
    result = load_organisations(transformed_data)

    logger.info(
        f"Organisation import complete. Deleted: {result['deleted']}, "
        f"Created: {result['created']}, Updated successors: {result['updated_successors']}"
    )

    return result


if __name__ == "__main__":
    load_organisations_flow()
