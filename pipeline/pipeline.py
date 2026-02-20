import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

import argparse

from prefect import flow, get_run_logger
from pipeline.setup.setup_bq_tables import setup_tables
from pipeline.dmd.import_dmd_base import import_dmd_base
from pipeline.dmd.import_dmd_filtered import import_dmd_filtered
from pipeline.organisations.import_organisations import import_organisations
from pipeline.scmd.import_scmd import import_scmd
from pipeline.atc_ddd.import_atc_ddd.import_atc_ddd import import_atc_ddd
from pipeline.quantity.check_quantity_calculations import check_quantity_calculations
from pipeline.quantity.calculate_quantities import calculate_quantities
from pipeline.aware.create_aware_vmp_mapping_processed import create_aware_vmp_mapping
from pipeline.mappings.import_mappings import import_mappings
from pipeline.scmd.process_scmd import process_scmd

from pipeline.load_data.load_data import load_data
from pipeline.measures.generate_measures import generate_measures
from pipeline.submission_history.update_submission_history_cache import update_submission_history_cache
from pipeline.utils.vacuum_tables import vacuum_tables

from pipeline.atc_ddd.ddd_comments.populate_ddd_refers_to import populate_ddd_refers_to_table
from pipeline.products.populate_vmp_table import populate_vmp_table


@flow(name="SCMD Import Pipeline")
def scmd_pipeline(run_import_flows: bool = True, run_load_flows: bool = True):
    logger = get_run_logger()
    logger.info("Starting SCMD Import Pipeline")

    if run_import_flows:
        logger.info("Starting import flows")

        setup_tables()
        import_dmd_base()
        import_organisations()
        import_scmd()
        import_atc_ddd()
        import_mappings()
        process_scmd()
        import_dmd_filtered()
        populate_vmp_table()
        check_quantity_calculations()
        calculate_quantities()
        create_aware_vmp_mapping()
        logger.info("Import flows completed")


    if run_load_flows:
        logger.info("Starting load flows")
        
        try:
            load_data()
            generate_measures()
            update_submission_history_cache()
            vacuum_tables()

        except Exception as e:
            logger.error(f"Error during load flows: {e}")
            raise e

    logger.info("SCMD Import Pipeline completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SCMD Pipeline Runner")
    parser.add_argument(
        "--skip-import",
        action="store_false",
        dest="run_import",
        help="Skip import flows",
    )
    parser.add_argument(
        "--skip-load", action="store_false", dest="run_load", help="Skip load flows"
    )
    args = parser.parse_args()

    scmd_pipeline(
        run_import_flows=args.run_import, 
        run_load_flows=args.run_load
    )
