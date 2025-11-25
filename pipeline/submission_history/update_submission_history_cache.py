import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

from prefect import flow, get_run_logger
from pipeline.utils.utils import setup_django_environment

setup_django_environment()
from viewer.management.commands.update_org_submission_cache import update_org_submission_cache

@flow(name="Update Organisation Submission Cache")
def update_submission_history_cache():
    logger = get_run_logger()
    logger.info("Updating organisation submission cache")

    try:
        update_org_submission_cache()
        logger.info("Successfully updated organisation submission cache")
    except Exception as e:
        logger.error(f"Error updating organisation submission cache: {e}")
        raise e

if __name__ == "__main__":
    update_submission_history_cache()