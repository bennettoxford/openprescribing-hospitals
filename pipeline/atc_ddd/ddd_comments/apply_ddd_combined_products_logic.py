from pathlib import Path

from prefect import flow, get_run_logger

from pipeline.utils.utils import execute_bigquery_query_from_sql_file


@flow(name="Apply DDD combined products logic to calculation logic")
def apply_ddd_combined_products_logic():
    """
    Overwrite rows in the DDD calculation logic table for VMPs that appear in the
    combined products logic table:
    - Where why_ddd_not_chosen is null: set selected DDD value/unit/basis unit,
      can_calculate_ddd = true, ddd_calculation_logic = "Calculated using DDD for combined product".
    - Where why_ddd_not_chosen is not null: set ddd_calculation_logic to
      "Not calculated: {why_ddd_not_chosen}".
    """
    logger = get_run_logger()
    logger.info("Applying DDD combined products logic to DDD calculation logic table")

    sql_file_path = Path(__file__).parent / "apply_ddd_combined_products_logic.sql"
    execute_bigquery_query_from_sql_file(str(sql_file_path))

    logger.info("DDD calculation logic table updated from combined products logic")


if __name__ == "__main__":
    apply_ddd_combined_products_logic()
