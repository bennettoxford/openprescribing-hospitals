from prefect import flow, get_run_logger
from pipeline.utils.utils import execute_bigquery_query_from_sql_file, validate_table_schema
from pipeline.setup.bq_tables import VMP_TABLE_SPEC
from pathlib import Path


@flow(name="Populate VMP Table")
def populate_vmp_table():
    logger = get_run_logger()
    logger.info("Populating VMP table")

    sql_file_path = Path(__file__).parent / "populate_vmp_table.sql"

    execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("VMP table populated successfully")
    
    validate_table_schema(VMP_TABLE_SPEC)
    logger.info("VMP table schema validation completed")
    


if __name__ == "__main__":
    populate_vmp_table() 