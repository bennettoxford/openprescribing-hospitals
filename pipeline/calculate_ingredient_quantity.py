from prefect import flow, get_run_logger
from utils import execute_bigquery_query_from_sql_file
from pathlib import Path

@flow(name="Calculate ingredient quantities")
def calculate_ingredient_quantity():
    logger = get_run_logger()
    logger.info("Calculating ingredient quantities")
    
    sql_file_path = Path(__file__).parent / "sql" / "calculate_ingredient_quantity.sql"
    
    execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("Ingredient quantities calculated")

if __name__ == "__main__":
    calculate_ingredient_quantity() 