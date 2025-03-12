from prefect import flow, get_run_logger

from setup_bq_tables import setup_tables
from import_unit_conversion import import_unit_conversion_flow
from import_organisations import import_organisations
from import_scmd import scmd_import
from import_org_ae_status import import_ae_status
from import_adm_route_mapping import import_adm_route_mapping_flow
from import_dmd import import_dmd
from process_scmd import process_scmd
from import_atc_ddd import import_ddd_atc_flow
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
    scmd_import()
    process_scmd()
    logger.info("SCMD Import Pipeline completed")
    
if __name__ == "__main__":
    scmd_pipeline()
