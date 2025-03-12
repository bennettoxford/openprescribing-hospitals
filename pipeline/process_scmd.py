from prefect import flow, task, get_run_logger
from utils import execute_bigquery_query_from_sql_file, get_bigquery_client
from pathlib import Path
from config import PROJECT_ID, DATASET_ID, ORGANISATION_TABLE_ID, UNITS_CONVERSION_TABLE_ID

@task
def validate_processed_data():
    """Validate the processed SCMD data"""
    logger = get_run_logger()
    client = get_bigquery_client()

    org_validation_query = f"""
    WITH scmd_orgs AS (
        SELECT DISTINCT ods_code
        FROM `{PROJECT_ID}.{DATASET_ID}.scmd_processed`
    )
    SELECT 
        s.ods_code,
        COUNT(*) as count
    FROM scmd_orgs s
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{ORGANISATION_TABLE_ID}` o
        ON s.ods_code = o.ods_code
    WHERE o.ods_code IS NULL
    GROUP BY s.ods_code
    """

    units_validation_query = f"""
    WITH scmd_units AS (
        SELECT DISTINCT 
            CAST(CAST(normalised_uom_id AS STRING) AS STRING) as unit  -- Double cast to ensure string
        FROM `{PROJECT_ID}.{DATASET_ID}.scmd_processed`
    )
    SELECT 
        s.unit,
        COUNT(*) as count
    FROM scmd_units s
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{UNITS_CONVERSION_TABLE_ID}` u
        ON CAST(s.unit AS STRING) = CAST(u.basis_id AS STRING)
    WHERE u.basis_id IS NULL
    GROUP BY s.unit
    """

    missing_orgs = list(client.query(org_validation_query).result())
    missing_units = list(client.query(units_validation_query).result())
    
    if missing_orgs:
        logger.error(f"Found {len(missing_orgs)} organisations in SCMD not in reference table:")
        for row in missing_orgs[:5]:
            logger.error(f"- {row.ods_code}: {row.count} occurrences")
        raise ValueError("Invalid organisations found in processed SCMD data")
        
    if missing_units:
        logger.error(f"Found {len(missing_units)} units in SCMD not in reference table:")
        for row in missing_units[:5]:
            logger.error(f"- {row.unit}: {row.count} occurrences")
        raise ValueError("Invalid units found in processed SCMD data")
    
    logger.info("All validations passed")

@flow(name="Process SCMD")
def process_scmd():
    logger = get_run_logger()
    logger.info("Processing SCMD")
    
    sql_file_path = Path(__file__).parent / "sql" / "process_scmd.sql"
    
    execute_bigquery_query_from_sql_file(str(sql_file_path))
    
    validate_processed_data()
    
    logger.info("SCMD processed and validated")

if __name__ == "__main__":
    process_scmd()

