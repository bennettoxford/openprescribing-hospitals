import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

from prefect import flow, get_run_logger
from pipeline.mappings.import_adm_route_mapping import import_adm_route_mapping
from pipeline.mappings.import_unit_conversion import import_unit_conversion
from pipeline.mappings.import_vmp_unit_standardisation import import_vmp_unit_standardisation

@flow(name="Import Mappings")
def import_mappings():
    logger = get_run_logger()
    logger.info("Starting Import Mappings")

    import_adm_route_mapping()
    import_unit_conversion()
    import_vmp_unit_standardisation()


if __name__ == "__main__":
    import_mappings()