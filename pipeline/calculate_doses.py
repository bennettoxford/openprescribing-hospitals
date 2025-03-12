from prefect import flow, get_run_logger
from utils import execute_bigquery_query_from_sql_file
from pathlib import Path

@flow(name="Calculate doses")
def calculate_doses():
    logger = get_run_logger()
    logger.info("Calculating doses")
    
    # Get the directory where this script is located
    sql_file_path = Path(__file__).parent / "sql" / "calculate_doses.sql"
    
    execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("doses calculated")

if __name__ == "__main__":
    calculate_doses()

