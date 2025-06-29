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
from viewer.models import (
    Organisation, 
    SCMDQuantity, 
    Dose, 
    IngredientQuantity, 
    DDDQuantity, 
    IndicativeCost, 
    PrecomputedMeasure, 
    OrgSubmissionCache
)


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
    logger = get_run_logger()
    logger.info(f"Loading {len(data)} organisation records")

    with transaction.atomic():
        logger.info("Deleting SCMDQuantity records...")
        scmd_deleted_total = 0
        while SCMDQuantity.objects.exists():
            ids = SCMDQuantity.objects.values_list('id', flat=True)[:10000]
            batch_count = SCMDQuantity.objects.filter(id__in=ids).delete()[0]
            scmd_deleted_total += batch_count
            logger.info(f"Deleted {batch_count} SCMDQuantity records (total: {scmd_deleted_total})")
        logger.info(f"Finished deleting SCMDQuantity records. Total deleted: {scmd_deleted_total}")
        
        logger.info("Deleting Dose records...")
        dose_deleted_total = 0
        while Dose.objects.exists():
            ids = Dose.objects.values_list('id', flat=True)[:10000]
            batch_count = Dose.objects.filter(id__in=ids).delete()[0]
            dose_deleted_total += batch_count
        logger.info(f"Finished deleting Dose records. Total deleted: {dose_deleted_total}")
        
        logger.info("Deleting IngredientQuantity records...")
        ingredient_deleted_total = 0
        while IngredientQuantity.objects.exists():
            ids = IngredientQuantity.objects.values_list('id', flat=True)[:10000]
            batch_count = IngredientQuantity.objects.filter(id__in=ids).delete()[0]
            ingredient_deleted_total += batch_count
        logger.info(f"Finished deleting IngredientQuantity records. Total deleted: {ingredient_deleted_total}")

        logger.info("Deleting DDDQuantity records...")
        ddd_deleted_total = 0
        while DDDQuantity.objects.exists():
            ids = DDDQuantity.objects.values_list('id', flat=True)[:10000]
            batch_count = DDDQuantity.objects.filter(id__in=ids).delete()[0]
            ddd_deleted_total += batch_count
        logger.info(f"Finished deleting DDDQuantity records. Total deleted: {ddd_deleted_total}")
        
        logger.info("Deleting IndicativeCost records...")
        cost_deleted_total = 0
        while IndicativeCost.objects.exists():
            ids = IndicativeCost.objects.values_list('id', flat=True)[:10000]
            batch_count = IndicativeCost.objects.filter(id__in=ids).delete()[0]
            cost_deleted_total += batch_count
        logger.info(f"Finished deleting IndicativeCost records. Total deleted: {cost_deleted_total}")
        
        logger.info("Deleting PrecomputedMeasure records...")
        measure_deleted_total = 0
        while PrecomputedMeasure.objects.exists():
            ids = PrecomputedMeasure.objects.values_list('id', flat=True)[:10000]
            batch_count = PrecomputedMeasure.objects.filter(id__in=ids).delete()[0]
            measure_deleted_total += batch_count
        logger.info(f"Finished deleting PrecomputedMeasure records. Total deleted: {measure_deleted_total}")
        
        logger.info("Deleting OrgSubmissionCache records...")
        cache_deleted_total = 0
        while OrgSubmissionCache.objects.exists():
            ids = OrgSubmissionCache.objects.values_list('id', flat=True)[:10000]
            batch_count = OrgSubmissionCache.objects.filter(id__in=ids).delete()[0]
            cache_deleted_total += batch_count
        logger.info(f"Finished deleting OrgSubmissionCache records. Total deleted: {cache_deleted_total}")

        logger.info("Deleting Organisation records...")
        deleted_count = Organisation.objects.all().delete()[0]
        logger.info(f"Deleted {deleted_count} Organisation records")

        total_related_deleted = (scmd_deleted_total + dose_deleted_total + ingredient_deleted_total + 
                               ddd_deleted_total + cost_deleted_total + measure_deleted_total + cache_deleted_total)
        logger.info(f"Deletion summary - Related records: {total_related_deleted}, Organisations: {deleted_count}")
        logger.info("Deletion phase complete")
     
    with transaction.atomic():
        logger.info("Starting creation phase...")

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
    

    with transaction.atomic():
        logger.info("Starting successor updates...")
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
        logger.info("Successor updates complete")

    logger.info(
        f"Organisation data load complete. Related deleted: {total_related_deleted}, "
        f"Organisations deleted: {deleted_count}, Created: {total_created}, "
        f"Updated successors: {successor_updates}"
    )
    return {
        "related_records_deleted": total_related_deleted,
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
        f"Organisation import complete. Related deleted: {result['related_records_deleted']}, "
        f"Organisations deleted: {result['deleted']}, Created: {result['created']}, "
        f"Updated successors: {result['updated_successors']}"
    )

    return result


if __name__ == "__main__":
    load_organisations_flow()
