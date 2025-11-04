import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

from prefect import flow, get_run_logger
from pipeline.organisations.import_ord_data import import_ord_data
from pipeline.organisations.import_eric import import_eric_data
from pipeline.organisations.import_org_ae_status import import_org_ae_status

@flow(name="Import Organisations")
def import_organisations():
    logger = get_run_logger()
    logger.info("Starting Organisations Import")

    import_ord_data()
    import_eric_data()
    import_org_ae_status()


if __name__ == "__main__":
    import_organisations()