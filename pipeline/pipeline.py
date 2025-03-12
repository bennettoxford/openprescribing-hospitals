from prefect import flow, get_run_logger

from setup_bq_tables import setup_tables
from import_unit_conversion import import_unit_conversion_flow
@flow(name="SCMD Import Pipeline")
def scmd_pipeline():
    logger = get_run_logger()
    logger.info("Starting SCMD Import Pipeline")
    setup_tables()
    import_unit_conversion_flow()
    logger.info("SCMD Import Pipeline completed")
    
if __name__ == "__main__":
    scmd_pipeline()
