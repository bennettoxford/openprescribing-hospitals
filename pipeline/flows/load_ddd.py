from prefect import get_run_logger, task, flow
from django.db import transaction
from typing import List, Dict
from pipeline.utils.utils import setup_django_environment, fetch_table_data_from_bq
from pipeline.bq_tables import VMP_DDD_MAPPING_TABLE_SPEC

setup_django_environment()

from viewer.models import DDD, VMP, WHORoute


@task()
def extract_ddd_data() -> List[Dict]:
    """Extract DDD data from BigQuery VMP_DDD_MAPPING table using selected DDD values"""
    logger = get_run_logger()
    logger.info("Starting extraction of DDD data from BigQuery VMP_DDD_MAPPING table")

    df = fetch_table_data_from_bq(VMP_DDD_MAPPING_TABLE_SPEC)

    filtered_df = df[
        (df["selected_ddd_value"].notna())
        & (df["selected_ddd_value"] > 0)
        & (df["selected_ddd_unit"].notna())
        & (df["selected_ddd_route_code"].notna())
    ]

    data = []
    for _, row in filtered_df.iterrows():
        data.append(
            {
                "vmp_code": row["vmp_code"],
                "vmp_name": row["vmp_name"],
                "ddd": row["selected_ddd_value"],
                "ddd_unit": row["selected_ddd_unit"],
                "who_route": row["selected_ddd_route_code"],
            }
        )

    logger.info(f"Extracted {len(data)} DDD records from BigQuery")
    return data


@task
def transform_ddd_data(data: List[Dict]) -> List[Dict]:
    """Transform the DDD data to match Django model structure"""
    logger = get_run_logger()
    logger.info("Transforming DDD data")

    transformed_data = []

    for row in data:
        if (
            row.get("vmp_code")
            and row.get("ddd") is not None
            and row.get("ddd_unit")
            and row.get("who_route")
        ):
            transformed_data.append(
                {
                    "vmp_code": row["vmp_code"],
                    "vmp_name": row["vmp_name"],
                    "ddd": float(row["ddd"]),
                    "unit_type": row["ddd_unit"],
                    "who_route": row["who_route"],
                }
            )

    logger.info(f"Transformed {len(transformed_data)} DDD records")
    return transformed_data


@task
def load_ddd_data(data: List[Dict]) -> Dict:
    """Load DDD data by replacing all existing data"""
    logger = get_run_logger()
    logger.info(f"Loading {len(data)} DDD records")

    who_routes = set(row["who_route"] for row in data)
    existing_routes = WHORoute.objects.filter(code__in=who_routes)
    found_routes = set(route.code for route in existing_routes)

    missing_routes = who_routes - found_routes
    if missing_routes:
        raise ValueError(
            f"Missing WHO routes in database: {missing_routes}. These routes must be created before running this script."
        )

    who_route_dict = {route.code: route for route in existing_routes}
    logger.info(f"Found all {len(who_routes)} required WHO routes in the database")

    vmp_codes = set(row["vmp_code"] for row in data)
    vmps = VMP.objects.filter(code__in=vmp_codes)
    vmp_dict = {vmp.code: vmp for vmp in vmps}

    missing_vmps = vmp_codes - set(vmp_dict.keys())
    if missing_vmps:
        logger.warning(
            f"Missing VMPs in database: {len(missing_vmps)} out of {len(vmp_codes)} total"
        )

    logger.info(
        f"Found {len(vmp_dict)} VMPs in the database out of {len(vmp_codes)} in the DDD data"
    )

    with transaction.atomic():
        deleted_count = DDD.objects.all().delete()[0]
        logger.info(f"Deleted {deleted_count} existing DDD records")

        ddd_objects = []
        skipped_count = 0

        for row in data:
            vmp_code = row["vmp_code"]
            ddd_value = row["ddd"]
            unit_type = row["unit_type"]
            who_route_code = row["who_route"]

            vmp = vmp_dict.get(vmp_code)
            if not vmp:
                skipped_count += 1
                continue

            who_route_obj = who_route_dict.get(who_route_code)
            if not who_route_obj:
                skipped_count += 1
                continue

            ddd_objects.append(
                DDD(
                    vmp=vmp, ddd=ddd_value, unit_type=unit_type, who_route=who_route_obj
                )
            )

        created_objects = DDD.objects.bulk_create(ddd_objects, batch_size=1000)
        total_created = len(created_objects)

        if skipped_count > 0:
            logger.warning(
                f"Skipped {skipped_count} DDD records due to missing VMP or WHO route references"
            )

    logger.info(
        f"DDD data load complete. Deleted: {deleted_count}, Created: {total_created}, Skipped: {skipped_count}"
    )
    return {
        "deleted": deleted_count,
        "created": total_created,
        "skipped": skipped_count,
        "total_records": len(data),
    }


@flow
def load_ddd_flow():
    """Main flow to import DDD data from BigQuery to Django"""
    logger = get_run_logger()
    logger.info("Starting DDD import flow")

    ddd_data = extract_ddd_data()
    transformed_data = transform_ddd_data(ddd_data)
    result = load_ddd_data(transformed_data)

    logger.info(
        f"DDD import complete. Deleted: {result['deleted']}, Created: {result['created']}, Skipped: {result['skipped']}"
    )
    return result


if __name__ == "__main__":
    load_ddd_flow()
