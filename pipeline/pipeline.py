import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

import argparse

from prefect import flow, get_run_logger
from pipeline.utils.maintenance import enable_maintenance_mode, disable_maintenance_mode
from pipeline.flows.setup_bq_tables import setup_tables
from pipeline.flows.import_unit_conversion import import_unit_conversion_flow
from pipeline.flows.import_organisations import import_organisations
from pipeline.flows.import_scmd import scmd_import
from pipeline.flows.import_scmd_pre_apr_2019 import import_scmd_pre_april_2019
from pipeline.flows.import_org_ae_status import import_ae_status
from pipeline.flows.import_adm_route_mapping import import_adm_route_mapping_flow
from pipeline.flows.import_dmd import import_dmd
from pipeline.flows.import_dmd_uom import import_dmd_uom
from pipeline.flows.import_dmd_supp import import_dmd_supp_flow
from pipeline.flows.populate_vmp_table import populate_vmp_table
from pipeline.flows.import_vmp_unit_standardisation import import_vmp_unit_standardisation_flow
from pipeline.flows.process_scmd import process_scmd
from pipeline.flows.calculate_doses import calculate_doses
from pipeline.flows.calculate_ingredient_quantity import calculate_ingredient_quantity
from pipeline.flows.import_atc_ddd_alterations import import_atc_ddd_alterations_flow
from pipeline.flows.import_atc import import_atc_flow
from pipeline.flows.import_ddd import import_ddd_flow
from pipeline.flows.create_vmp_ddd_mapping import create_vmp_ddd_mapping
from pipeline.flows.load_organisations import load_organisations_flow
from pipeline.flows.load_data_status import load_data_status_flow
from pipeline.flows.calculate_ddd_quantity import calculate_ddd_quantity

@flow(name="SCMD Import Pipeline")
def scmd_pipeline(run_import_flows: bool = True, run_load_flows: bool = True):
    logger = get_run_logger()
    logger.info("Starting SCMD Import Pipeline")

    if run_import_flows:
        logger.info("Starting import flows")

        setup_result = setup_tables()

        unit_conv = import_unit_conversion_flow(wait_for=[setup_result])
        org_result = import_organisations(wait_for=[setup_result])
        ae_status_result = import_ae_status(wait_for=[setup_result])
        atc_ddd_alterations = import_atc_ddd_alterations_flow(wait_for=[setup_result])

        atc_result = import_atc_flow(wait_for=[atc_ddd_alterations])
        ddd_result = import_ddd_flow(wait_for=[atc_ddd_alterations])

        adm_route = import_adm_route_mapping_flow(wait_for=[ae_status_result])
        dmd_result = import_dmd(wait_for=[adm_route])
        dmd_uom = import_dmd_uom(wait_for=[dmd_result])
        dmd_supp = import_dmd_supp_flow(wait_for=[dmd_uom, atc_ddd_alterations])
    
        vmps = populate_vmp_table(wait_for=[dmd_supp])
        vmp_unit_standardisation = import_vmp_unit_standardisation_flow(wait_for=[vmps])
        

        scmd_pre_apr_result = import_scmd_pre_april_2019(wait_for=[vmp_unit_standardisation])
        scmd_result = scmd_import(wait_for=[scmd_pre_apr_result])
        
        processed = process_scmd(wait_for=[scmd_result, unit_conv, org_result])
        doses = calculate_doses(wait_for=[processed])
        ingredients = calculate_ingredient_quantity(wait_for=[doses])
    
        ddd_mapping = create_vmp_ddd_mapping(wait_for=[ingredients, atc_result, ddd_result])
        ddd_quantities = calculate_ddd_quantity(wait_for=[ddd_mapping])

        logger.info("Import flows completed")
        last_import_result = ddd_quantities
    else:
        last_import_result = None

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
            status = load_data_status_flow(wait_for=[last_import_result])
            load_organisations_flow(wait_for=[status])

            logger.info("Load flows completed")
            
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
    return last_import_result

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
