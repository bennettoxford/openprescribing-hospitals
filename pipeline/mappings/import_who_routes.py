import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

from prefect import flow, get_run_logger
from pipeline.mappings.import_adm_route_mapping import import_adm_route_mapping
from pipeline.mappings.import_vmp_manual_who_route import import_vmp_manual_who_route


@flow(name="Import WHO routes")
def import_who_routes():
    """Load WHO route maps after the filtered dm+d table exists."""
    logger = get_run_logger()
    logger.info("Starting WHO route import")
    import_adm_route_mapping()
    import_vmp_manual_who_route()


if __name__ == "__main__":
    import_who_routes()
