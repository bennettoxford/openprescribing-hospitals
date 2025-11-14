import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

from prefect import flow, get_run_logger
from pipeline.quantity.dose_calculation_logic import dose_calculation_logic
from pipeline.quantity.ingredient_calculation_logic import ingredient_calculation_logic
from pipeline.quantity.ddd_calculation_logic import ddd_calculation_logic
from pipeline.atc_ddd.ddd_comments.populate_ddd_refers_to import populate_ddd_refers_to_table
from pipeline.quantity.ddd_comment_handling import ddd_comment_handling


@flow(name="Check Quantity Calculations")
def check_quantity_calculations():
    logger = get_run_logger()
    logger.info("Starting Check Quantity Calculations")

    dose_calculation_logic()
    ingredient_calculation_logic()
    ddd_calculation_logic()
    populate_ddd_refers_to_table()
    ddd_comment_handling()


if __name__ == "__main__":
    check_quantity_calculations()