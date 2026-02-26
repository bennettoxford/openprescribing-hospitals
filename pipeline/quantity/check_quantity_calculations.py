import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

from prefect import flow, get_run_logger
from pipeline.quantity.dose_calculation_logic import dose_calculation_logic
from pipeline.quantity.ingredient_calculation_logic import ingredient_calculation_logic
from pipeline.quantity.ddd_calculation_logic import ddd_calculation_logic
from pipeline.atc_ddd.ddd_comments.populate_ddd_route_comments import populate_ddd_route_comments_table
from pipeline.quantity.handle_ddd_route_comments import handle_ddd_route_comments
from pipeline.atc_ddd.ddd_comments.populate_vmp_strength_overrides import populate_vmp_strength_overrides_table
from pipeline.atc_ddd.ddd_comments.populate_ddd_refers_to import populate_ddd_refers_to_table
from pipeline.quantity.ddd_comment_handling_refers_to import ddd_comment_handling_refers_to
from pipeline.atc_ddd.ddd_comments.populate_ddd_expressed_as import populate_ddd_expressed_as_table
from pipeline.atc_ddd.ddd_comments.handle_ddd_comments_expressed_as import handle_ddd_comments_expressed_as
from pipeline.atc_ddd.ddd_comments.import_ddd_combined_products import import_ddd_combined_products
from pipeline.atc_ddd.ddd_comments.populate_ddd_combined_products_logic import populate_ddd_combined_products_logic
from pipeline.atc_ddd.ddd_comments.apply_ddd_combined_products_logic import apply_ddd_combined_products_logic


@flow(name="Check Quantity Calculations")
def check_quantity_calculations():
    logger = get_run_logger()
    logger.info("Starting Check Quantity Calculations")

    dose_calculation_logic()
    ingredient_calculation_logic()
    populate_vmp_strength_overrides_table()
    ddd_calculation_logic()
    populate_ddd_route_comments_table()
    handle_ddd_route_comments()
    populate_ddd_refers_to_table()
    ddd_comment_handling_refers_to()
    populate_ddd_expressed_as_table()
    handle_ddd_comments_expressed_as()
    import_ddd_combined_products()
    populate_ddd_combined_products_logic()
    apply_ddd_combined_products_logic()


if __name__ == "__main__":
    check_quantity_calculations()