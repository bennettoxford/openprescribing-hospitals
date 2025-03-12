from prefect import flow, task, get_run_logger
from utils import execute_bigquery_query_from_sql_file
from pathlib import Path
from google.cloud import bigquery
from config import PROJECT_ID, DATASET_ID, ADM_ROUTE_MAPPING_TABLE_ID
from utils import get_bigquery_client

@task
def validate_routes(client: bigquery.Client):
    logger = get_run_logger()
    
    query = f"""
    WITH unique_routes AS (
        SELECT DISTINCT route_descr
        FROM `{PROJECT_ID}.{DATASET_ID}.dmd`,
        UNNEST(routes)
    )
    SELECT route_descr
    FROM unique_routes
    WHERE route_descr NOT IN (
        SELECT dmd_route
        FROM `{PROJECT_ID}.{DATASET_ID}.{ADM_ROUTE_MAPPING_TABLE_ID}`
    )
    """
    
    results = client.query(query).result()
    missing_routes = [row.route_descr for row in results]
    
    if missing_routes:
        logger.error(f"Found routes in DMD that are not in route mapping table: {missing_routes}")
        raise ValueError("Missing route mappings detected")
    
    logger.info("All DMD routes are present in route mapping table")

@flow(name="Import dm+d")
def import_dmd():
    logger = get_run_logger()
    logger.info("Importing dm+d")
    
    sql_file_path = Path(__file__).parent / "sql" / "import_dmd.sql"
    
    execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("dm+d imported")
    
    
    client = get_bigquery_client()
    validate_routes(client)

if __name__ == "__main__":
    import_dmd()

