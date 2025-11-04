import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

from prefect import flow, get_run_logger
from pipeline.scmd.import_scmd_pre_apr_2019 import import_scmd_pre_apr_2019
from pipeline.scmd.import_scmd_post_apr_2019 import import_scmd_post_apr_2019

@flow(name="Import SCMD")
def import_scmd():
    logger = get_run_logger()
    logger.info("Starting SCMD Import")

    # import_scmd_pre_apr_2019()
    import_scmd_post_apr_2019()


if __name__ == "__main__":
    import_scmd()