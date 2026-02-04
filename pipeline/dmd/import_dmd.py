import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

from prefect import flow, get_run_logger

from pipeline.dmd.import_dmd_main import import_dmd_main
from pipeline.dmd.import_dmd_full import import_dmd_full
from pipeline.dmd.import_dmd_uom import import_dmd_uom
from pipeline.dmd.import_dmd_supp import import_dmd_supp

@flow(name="Import dm+d")
def import_dmd():
    logger = get_run_logger()
    logger.info("Starting dm+d Import")

    import_dmd_full()
    import_dmd_main()
    import_dmd_uom()
    import_dmd_supp()

if __name__ == "__main__":
    import_dmd()