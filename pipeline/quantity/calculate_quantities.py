import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

from prefect import flow, get_run_logger

from pipeline.quantity.calculate_doses import calculate_doses
from pipeline.quantity.calculate_ingredient_quantity import calculate_ingredient_quantity
from pipeline.quantity.calculate_ddd_quantity import calculate_ddd_quantity

@flow(name="Calculate Quantities")
def calculate_quantities():
    logger = get_run_logger()
    logger.info("Starting Calculate Quantities")

    calculate_doses()
    calculate_ingredient_quantity()
    calculate_ddd_quantity()
    

if __name__ == "__main__":
    calculate_quantities()