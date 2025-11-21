from prefect import flow, get_run_logger
from pipeline.utils.utils import execute_bigquery_query_from_sql_file, validate_table_schema, get_bigquery_client
from pipeline.setup.bq_tables import VMP_TABLE_SPEC
from pipeline.setup.config import PROJECT_ID, DATASET_ID, SCMD_PROCESSED_TABLE_ID
from pathlib import Path


@flow(name="Populate VMP Table")
def populate_vmp_table():
    logger = get_run_logger()
    
    validate_single_unit_per_vmp()
    
    logger.info("Populating VMP table")
    sql_file_path = Path(__file__).parent / "populate_vmp_table.sql"

    execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("VMP table populated successfully")
    
    validate_table_schema(VMP_TABLE_SPEC)
    logger.info("VMP table schema validation completed")
    

def validate_single_unit_per_vmp():
    """Validate that each VMP uses a single unit across all months and trusts in SCMD data"""
    logger = get_run_logger()
    logger.info("Validating that each VMP uses a single unit in SCMD data")
    
    client = get_bigquery_client()
    
    validation_query = f"""
    WITH vmp_unit_check AS (
      SELECT
        vmp_code,
        vmp_name,
        ARRAY_AGG(DISTINCT normalised_uom_id ORDER BY normalised_uom_id) AS distinct_uom_ids,
        ARRAY_AGG(DISTINCT normalised_uom_name ORDER BY normalised_uom_name) AS distinct_uom_names,
        COUNT(DISTINCT normalised_uom_id) AS num_distinct_units
      FROM `{PROJECT_ID}.{DATASET_ID}.{SCMD_PROCESSED_TABLE_ID}`
      GROUP BY vmp_code, vmp_name
      HAVING num_distinct_units > 1
      ORDER BY num_distinct_units DESC
      LIMIT 10
    )
    SELECT 
        vmp_code,
        vmp_name,
        num_distinct_units,
        distinct_uom_names
    FROM vmp_unit_check
    """
    
    results = client.query(validation_query).result()
    violations = list(results)
    
    if violations:
        error_details = "\n".join([
            f"  - VMP {row.vmp_code} ({row.vmp_name}): {row.num_distinct_units} distinct units ({', '.join(row.distinct_uom_names)})"
            for row in violations
        ])
        
        total_violations_query = f"""
        SELECT COUNT(*) as count
        FROM (
          SELECT vmp_code
          FROM `{PROJECT_ID}.{DATASET_ID}.{SCMD_PROCESSED_TABLE_ID}`
          GROUP BY vmp_code
          HAVING COUNT(DISTINCT normalised_uom_id) > 1
        )
        """
        total_count = list(client.query(total_violations_query).result())[0].count
        
        error_message = (
            f"Validation failed: {total_count} VMP(s) have multiple units in SCMD data.\n"
            f"First 10 examples:\n{error_details}"
        )
        logger.error(error_message)
        raise ValueError(error_message)
    
    logger.info("Validation passed: All VMPs use a single unit consistently")


if __name__ == "__main__":
    populate_vmp_table() 