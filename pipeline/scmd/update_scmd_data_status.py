from prefect import flow, task, get_run_logger
from google.cloud import bigquery

from pipeline.utils.utils import get_bigquery_client
from pipeline.setup.bq_tables import (
    SCMD_RAW_PROVISIONAL_TABLE_SPEC,
    SCMD_RAW_FINALISED_TABLE_SPEC,
    SCMD_DATA_STATUS_TABLE_SPEC,
)


@task
def get_finalised_months() -> set:
    """Get all months that have finalised data"""
    logger = get_run_logger()
    client = get_bigquery_client()

    try:
        query = f"""
        SELECT DISTINCT year_month
        FROM {SCMD_RAW_FINALISED_TABLE_SPEC.full_table_id}
        ORDER BY year_month DESC
        """

        query_job = client.query(query)
        query_result = query_job.result()

        finalised_months = set()
        for row in query_result:
            date_val = row.year_month
            if hasattr(date_val, 'strftime'):
                finalised_months.add(
                    date_val.strftime("%Y-%m-%d")
                )
            else:
                finalised_months.add(str(date_val))

        logger.info(
            f"Found {len(finalised_months)} months with finalised data"
        )
        return finalised_months
    except Exception as e:
        logger.warning(
            f"Could not fetch finalised months from "
            f"{SCMD_RAW_FINALISED_TABLE_SPEC.full_table_id}: {e}"
        )
        return set()


@task
def get_provisional_months() -> set:
    """Get all months that have provisional data"""
    logger = get_run_logger()
    client = get_bigquery_client()

    try:
        query = f"""
        SELECT DISTINCT year_month
        FROM {SCMD_RAW_PROVISIONAL_TABLE_SPEC.full_table_id}
        ORDER BY year_month DESC
        """

        query_job = client.query(query)
        query_result = query_job.result()

        provisional_months = set()
        for row in query_result:
            date_val = row.year_month
            if hasattr(date_val, 'strftime'):
                provisional_months.add(date_val.strftime("%Y-%m-%d"))
            else:
                provisional_months.add(str(date_val))

        logger.info(
            f"Found {len(provisional_months)} months with provisional data"
        )
        return provisional_months
    except Exception as e:
        logger.warning(
            f"Could not fetch provisional months from "
            f"{SCMD_RAW_PROVISIONAL_TABLE_SPEC.full_table_id}: {e}"
        )
        return set()


@task
def get_existing_status_months() -> dict:
    """Get existing months and their status from the data status table"""
    logger = get_run_logger()
    client = get_bigquery_client()

    try:
        query = f"""
        SELECT DISTINCT year_month, file_type
        FROM {SCMD_DATA_STATUS_TABLE_SPEC.full_table_id}
        ORDER BY year_month DESC
        """

        query_job = client.query(query)
        query_result = query_job.result()

        status_dict = {}
        for row in query_result:
            date_val = row.year_month
            date_str = (
                date_val.strftime("%Y-%m-%d")
                if hasattr(date_val, 'strftime')
                else str(date_val)
            )
            status_dict[date_str] = row.file_type

        logger.info(
            f"Found {len(status_dict)} months in data status table"
        )
        return status_dict
    except Exception as e:
        logger.warning(
            f"Could not fetch status months from "
            f"{SCMD_DATA_STATUS_TABLE_SPEC.full_table_id}: {e}"
        )
        return {}


@task
def update_data_status_table(
    finalised_months: set,
    provisional_months: set,
    existing_status: dict,
) -> dict:
    """Update the data status table with correct status for each month"""
    logger = get_run_logger()
    client = get_bigquery_client()

    # Determine which months should be finalised vs provisional
    # Finalised takes priority - if a month has both, it's finalised
    months_to_finalise = finalised_months.copy()
    months_to_provisional = (
        provisional_months - finalised_months
    )

    records = []
    all_months = months_to_finalise | months_to_provisional

    for month in sorted(all_months):
        status = "final" if month in months_to_finalise else "provisional"
        
        existing_status_for_month = existing_status.get(month)
        if existing_status_for_month != status:
            records.append({
                "year_month": month,
                "file_type": status,
            })

    if not records:
        logger.info("No status updates needed")
        return {
            "updated_months": 0,
            "finalised_months": len(months_to_finalise),
            "provisional_months": len(months_to_provisional),
            "status": "no_updates_needed",
        }

    logger.info(
        f"Updating status for {len(records)} months "
        f"({len(months_to_finalise)} finalised, "
        f"{len(months_to_provisional)} provisional)"
    )

    try:
        job_config = bigquery.LoadJobConfig(
            schema=SCMD_DATA_STATUS_TABLE_SPEC.schema,
            write_disposition="WRITE_TRUNCATE",
        )

        job = client.load_table_from_json(
            records,
            SCMD_DATA_STATUS_TABLE_SPEC.full_table_id,
            job_config=job_config
        )
        job.result()

        logger.info(
            f"Successfully updated status for {len(records)} months"
        )

        return {
            "updated_months": len(records),
            "failed_months": 0,
            "total_months": len(records),
            "finalised_months": len(months_to_finalise),
            "provisional_months": len(months_to_provisional),
            "status": "completed",
        }

    except Exception as e:
        logger.error(f"Error updating data status table: {e}")
        raise


@flow(name="Update SCMD Data Status")
def update_scmd_data_status():
    """Update the SCMD data status table based on actual data in
    provisional and finalised tables"""
    logger = get_run_logger()

    logger.info("Starting SCMD data status update")

    finalised_months = get_finalised_months()
    provisional_months = get_provisional_months()
    existing_status = get_existing_status_months()

    result = update_data_status_table(
        finalised_months, provisional_months, existing_status
    )

    logger.info(
        f"Status update complete: {result['updated_months']} months updated, "
        f"{result['finalised_months']} finalised, "
        f"{result['provisional_months']} provisional"
    )



if __name__ == "__main__":
    update_scmd_data_status()
