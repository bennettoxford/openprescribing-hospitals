import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

from prefect import flow, get_run_logger

from pipeline.dmd.import_dmd_full import import_dmd_full
from pipeline.dmd.import_dmd_uom import import_dmd_uom
from pipeline.dmd.import_dmd_supp import import_dmd_supp


@flow(name="Import dm+d base")
def import_dmd_base():
    logger = get_run_logger()
    logger.info("Starting dm+d base import")

    import_dmd_full()
    import_dmd_uom()
    import_dmd_supp()


if __name__ == "__main__":
    import_dmd_base()
