import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

import argparse

from prefect import flow, get_run_logger

from pipeline.atc_ddd.import_atc_ddd.import_atc import import_atc
from pipeline.atc_ddd.import_atc_ddd.import_ddd import import_ddd
from pipeline.atc_ddd.import_atc_ddd.import_atc_ddd_alterations import import_atc_ddd_alterations

@flow(name="Import ATC DDD")
def import_atc_ddd():
    logger = get_run_logger()
    logger.info("Starting ATC DDD Import")

    import_atc_ddd_alterations()
    import_atc()
    import_ddd()


if __name__ == "__main__":
    import_atc_ddd()