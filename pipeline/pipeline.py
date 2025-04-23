import logging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("prefect.client").setLevel(logging.WARNING)

import argparse

from prefect import flow, get_run_logger
from pipeline.flows.setup_bq_tables import setup_tables
from pipeline.flows.import_unit_conversion import import_unit_conversion_flow
from pipeline.flows.import_organisations import import_organisations
from pipeline.flows.import_scmd import scmd_import
from pipeline.flows.import_org_ae_status import import_ae_status
from pipeline.flows.import_adm_route_mapping import import_adm_route_mapping_flow
from pipeline.flows.import_dmd import import_dmd
from pipeline.flows.import_dmd_supp import import_dmd_supp_flow
from pipeline.flows.populate_vmp_table import populate_vmp_table
from pipeline.flows.import_vmp_unit_standardisation import import_vmp_unit_standardisation_flow
from pipeline.flows.process_scmd import process_scmd
from pipeline.flows.calculate_doses import calculate_doses
from pipeline.flows.calculate_ingredient_quantity import calculate_ingredient_quantity
from pipeline.flows.import_atc_ddd import import_ddd_atc_flow
from pipeline.flows.create_vmp_ddd_mapping import create_vmp_ddd_mapping
from pipeline.flows.calculate_ddd_quantity import calculate_ddd_quantity

@flow(name="SCMD Import Pipeline")
def scmd_pipeline(run_import_flows: bool = True):
    logger = get_run_logger()
    logger.info("Starting SCMD Import Pipeline")

    if run_import_flows:
        logger.info("Starting import flows")

        setup_result = setup_tables()

        unit_conv = import_unit_conversion_flow(wait_for=[setup_result])
        org_result = import_organisations(wait_for=[setup_result])
        ae_status_result = import_ae_status(wait_for=[setup_result])

        dmd_result = import_dmd(wait_for=[unit_conv])
        dmd_supp = import_dmd_supp_flow(wait_for=[dmd_result])
        adm_route = import_adm_route_mapping_flow(wait_for=[dmd_result])
        ddd_atc = import_ddd_atc_flow(wait_for=[adm_route])
        vmps = populate_vmp_table(wait_for=[adm_route])
        vmp_unit_standardisation = import_vmp_unit_standardisation_flow(wait_for=[vmps])
        scmd_result = scmd_import(wait_for=[vmp_unit_standardisation])
        processed = process_scmd(wait_for=[scmd_result, unit_conv, org_result])
        doses = calculate_doses(wait_for=[processed])
        ingredients = calculate_ingredient_quantity(wait_for=[doses])
        ddd_mapping = create_vmp_ddd_mapping(wait_for=[ingredients, ddd_atc])
        ddd_quantities = calculate_ddd_quantity(wait_for=[ddd_mapping])

        logger.info("Import flows completed")
        last_import_result = ddd_quantities
    else:
        last_import_result = None

    logger.info("SCMD Import Pipeline completed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='SCMD Pipeline Runner')
    parser.add_argument('--skip-import', action='store_false', dest='run_import',
                      help='Skip import flows')
    args = parser.parse_args()
    
    scmd_pipeline(
        run_import_flows=args.run_import
    )
