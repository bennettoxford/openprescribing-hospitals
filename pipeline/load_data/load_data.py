import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

from prefect import flow, get_run_logger

from pipeline.load_data.load_data_status import extract_and_load_data_status
from pipeline.load_data.load_organisations import load_organisations
from pipeline.load_data.load_atc import load_atc
from pipeline.load_data.load_vmp_vtm import load_vmp_vtm_data
from pipeline.load_data.load_aware_data import load_aware_data
from pipeline.load_data.load_ddd import load_ddd
from pipeline.load_data.load_indicative_cost import load_indicative_costs
from pipeline.load_data.load_dose_data import load_dose_data
from pipeline.load_data.load_ingredient_quantity import load_ingredient_quantity
from pipeline.load_data.load_ddd_quantity import load_ddd_quantity


@flow(name="Load Data")
def load_data():
    logger = get_run_logger()
    logger.info("Starting Load Data")

    extract_and_load_data_status()
    load_organisations()
    load_atc()
    load_vmp_vtm_data()
    load_aware_data()
    load_ddd()
    load_indicative_costs()
    load_dose_data()
    load_ingredient_quantity()
    load_ddd_quantity()

    logger.info("Load flows completed")


if __name__ == "__main__":
    load_data()