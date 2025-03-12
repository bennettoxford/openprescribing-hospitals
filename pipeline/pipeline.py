from prefect import flow, get_run_logger

from setup_bq_tables import setup_tables
from import_unit_conversion import import_unit_conversion_flow
from import_organisations import import_organisations
from import_scmd import scmd_import
from import_org_ae_status import import_ae_status
from import_adm_route_mapping import import_adm_route_mapping_flow
from import_dmd import import_dmd
from import_dmd_supp import import_dmd_supp_flow
from process_scmd import process_scmd
from calculate_doses import calculate_doses
from calculate_ingredient_quantity import calculate_ingredient_quantity
from import_atc_ddd import import_ddd_atc_flow
from vmp_ddd_mapping import create_vmp_ddd_mapping

@flow(name="SCMD Import Pipeline")
def scmd_pipeline():
    logger = get_run_logger()
    logger.info("Starting SCMD Import Pipeline")
    setup_tables()
    import_unit_conversion_flow()
    import_organisations()
    import_ae_status()
    import_adm_route_mapping_flow()
    import_ddd_atc_flow()
    import_dmd()
    import_dmd_supp_flow()
    scmd_import()
    process_scmd()
    calculate_doses()
    calculate_ingredient_quantity()
    create_vmp_ddd_mapping()
    logger.info("SCMD Import Pipeline completed")
    
if __name__ == "__main__":
    scmd_pipeline()
