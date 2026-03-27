from pathlib import Path

from prefect import flow, get_run_logger

from pipeline.utils.utils import execute_bigquery_query_from_sql_file, validate_table_schema
from pipeline.setup.bq_tables import DDD_COMBINED_PRODUCTS_LOGIC_TABLE_SPEC


@flow(name="Populate DDD combined products logic")
def populate_ddd_combined_products_logic():
    """
    Populate the ddd_combined_products_logic table with VMPs where standard DDD
    cannot be calculated, matched to WHO combined product DDDs, including reasons
    when a combined DDD could not be chosen.
    """
    logger = get_run_logger()
    logger.info("Populating DDD combined products logic table")

    sql_file_path = Path(__file__).parent / "populate_ddd_combined_products_logic.sql"
    execute_bigquery_query_from_sql_file(str(sql_file_path))

    logger.info("DDD combined products logic table populated")

    validate_table_schema(DDD_COMBINED_PRODUCTS_LOGIC_TABLE_SPEC)
    logger.info("DDD combined products logic table schema validation completed")


if __name__ == "__main__":
    populate_ddd_combined_products_logic()
