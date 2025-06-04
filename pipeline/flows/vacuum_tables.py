from prefect import flow, task, get_run_logger
from django.db import connection
from pipeline.utils.utils import setup_django_environment

setup_django_environment()

@task
def vacuum_tables():
    """Run VACUUM ANALYZE on specified tables or all tables if none specified"""
    logger = get_run_logger()
    
    with connection.cursor() as cursor:
        try:
            logger.info("Running VACUUM ANALYZE on all tables...")
            cursor.execute("VACUUM ANALYZE;")
            logger.info("VACUUM completed")
        except Exception as e:
            logger.error(f"Error running VACUUM ANALYZE: {e}")
            raise

@flow
def vacuum_tables_flow():
    """Flow to vacuum tables after data loading"""
    logger = get_run_logger()
    logger.info("Starting vacuum flow")
    
    vacuum_tables()

    logger.info("Vacuum flow completed")
    

if __name__ == "__main__":
    vacuum_tables_flow()
