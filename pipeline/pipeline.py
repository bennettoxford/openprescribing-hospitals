import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

import argparse

from prefect import flow, get_run_logger
from pipeline.utils.maintenance import enable_maintenance_mode, disable_maintenance_mode
from pipeline.setup.setup_bq_tables import setup_tables
from pipeline.dmd.import_dmd import import_dmd
from pipeline.organisations.import_organisations import import_organisations
from pipeline.scmd.import_scmd import import_scmd
from pipeline.atc_ddd.import_atc_ddd.import_atc_ddd import import_atc_ddd
from pipeline.quantity.calculate_quantities import calculate_quantities
from pipeline.aware.create_aware_vmp_mapping_processed import create_aware_vmp_mapping
from pipeline.mappings.import_mappings import import_mappings
from pipeline.scmd.process_scmd import process_scmd

from pipeline.load_data.load_data import load_data
from pipeline.measures.generate_measures import generate_measures
from pipeline.submission_history.update_submission_history_cache import update_submission_history_cache
from pipeline.utils.vacuum_tables import vacuum_tables

from pipeline.atc_ddd.ddd_comments.populate_ddd_refers_to import populate_ddd_refers_to_table
from pipeline.atc_ddd.atc_ddd_vmp_mapping.create_vmp_ddd_mapping import create_vmp_ddd_mapping
from pipeline.products.populate_vmp_table import populate_vmp_table


@flow(name="SCMD Import Pipeline")
def scmd_pipeline(run_import_flows: bool = True, run_load_flows: bool = True):
    logger = get_run_logger()
    logger.info("Starting SCMD Import Pipeline")

    if run_import_flows:
        logger.info("Starting import flows")

        setup_tables()
        import_dmd()
        import_organisations()
        import_scmd()
        import_atc_ddd()
        import_mappings()
        process_scmd()
        populate_ddd_refers_to_table()
        create_vmp_ddd_mapping()
        populate_vmp_table()
        calculate_quantities()
        create_aware_vmp_mapping()
        logger.info("Import flows completed")


    if run_load_flows:
        logger.info("Starting load flows")
        
        logger.info("Enabling maintenance mode before data loading")
        try:
            enable_maintenance_mode()      
            logger.info("MAINTENANCE MODE ENABLED")
        except Exception as e:
            logger.error(f"Failed to enable maintenance mode: {e}")
            raise Exception("Failed to enable maintenance mode")

        try:
            load_data()
            generate_measures()
            update_submission_history_cache()
            vacuum_tables()

        except Exception as e:
            logger.error(f"Error during load flows: {e}")
            raise e
        finally:
            logger.info("MAINTENANCE MODE - Disabling maintenance mode after data loading")
            try:
                disable_maintenance_mode()
            except Exception as e:
                logger.error(f"Failed to disable maintenance mode: {e}")
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
