from prefect import flow, task, get_run_logger
from google.api_core.exceptions import NotFound
from pathlib import Path

from pipeline.utils.utils import execute_bigquery_query_from_sql_file, get_bigquery_client, validate_table_schema
from pipeline.setup.bq_tables import AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC


@task
def create_aware_vmp_mapping_processed_table():
    """
    Create the processed AWaRe VMP mapping table with historical mapping applied 
    and restricted to VMPs in processed SCMD data.
    
    Returns:
        dict: Summary of the operation including row count
    """
    logger = get_run_logger()
    logger.info("Creating processed AWaRe VMP mapping table with historical mapping and SCMD restriction")
    
    try:
        result = execute_bigquery_query_from_sql_file(Path(__file__).parent / "create_aware_vmp_mapping_processed.sql")
        
        client = get_bigquery_client()
        table = client.get_table(AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC.full_table_id)
        row_count = table.num_rows
        
        logger.info(f"Successfully created table {AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC.full_table_id} with {row_count} rows")
        
        return {
            "table_id": AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC.full_table_id,
            "status": "created",
            "rows": row_count
        }
        
    except Exception as e:
        logger.error(f"Error creating processed AWaRe VMP mapping table: {str(e)}")
        raise


@task
def validate_aware_vmp_mapping_processed_table():
    """
    Validate the created processed AWaRe VMP mapping table by checking schema and data quality.
    
    Returns:
        dict: Validation results
    """
    logger = get_run_logger()
    logger.info("Validating processed AWaRe VMP mapping table")
    
    client = get_bigquery_client()
    
    try:
        schema_valid = validate_table_schema(AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC)
        if not schema_valid:
            logger.error("Schema validation failed for processed AWaRe VMP mapping table")
            return {
                "validation_passed": False,
                "issues": ["Schema validation failed"],
                "stats": {}
            }
        
        table = client.get_table(AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC.full_table_id)

        validation_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT Antibiotic) as unique_antibiotics,
            COUNT(DISTINCT vmp_id) as unique_vmps,
            COUNT(DISTINCT vtm_id) as unique_vtms,
            COUNTIF(aware_2019 IS NOT NULL) as rows_with_2019_classification,
            COUNTIF(aware_2024 IS NOT NULL) as rows_with_2024_classification,
            COUNTIF(vtm_id_updated = TRUE) as vtm_mappings_applied,
            COUNTIF(vmp_id_updated = TRUE) as vmp_mappings_applied
        FROM `{AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC.full_table_id}`
        """
        
        query_job = client.query(validation_query)
        results = list(query_job.result())
        
        if results:
            stats = dict(results[0])
            stats["schema_valid"] = schema_valid
            logger.info(f"Validation stats: {stats}")
            
            validation_passed = True
            issues = []
            
            if stats['total_rows'] == 0:
                validation_passed = False
                issues.append("Processed table is empty")
            
            if stats['unique_antibiotics'] == 0:
                validation_passed = False
                issues.append("No antibiotics found")
                
            if stats['unique_vmps'] == 0:
                validation_passed = False
                issues.append("No VMPs mapped")
            
            return {
                "validation_passed": validation_passed,
                "issues": issues,
                "stats": stats
            }
        else:
            return {
                "validation_passed": False,
                "issues": ["No results from validation query"],
                "stats": {"schema_valid": schema_valid}
            }
            
    except NotFound:
        logger.error(f"Table {AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC.full_table_id} not found")
        return {
            "validation_passed": False,
            "issues": ["Table not found"],
            "stats": {}
        }
    except Exception as e:
        logger.error(f"Error validating processed AWaRe VMP mapping table: {str(e)}")
        return {
            "validation_passed": False,
            "issues": [f"Validation error: {str(e)}"],
            "stats": {}
        }


@flow(name="Create Processed AWaRe VMP Mapping Table")
def create_aware_vmp_mapping():
    """
    Main flow to create the processed AWaRe VMP mapping table with historical mapping
    applied and restricted to VMPs present in processed SCMD data.
    """
    logger = get_run_logger()
    logger.info("Starting processed AWaRe VMP mapping table creation flow")
    
    try:
        creation_result = create_aware_vmp_mapping_processed_table()
        
        validation_result = validate_aware_vmp_mapping_processed_table()
        
        flow_result = {
            "status": "success" if validation_result["validation_passed"] else "warning",
            "creation": creation_result,
            "validation": validation_result,
            "message": f"Successfully created processed AWaRe VMP mapping table with {creation_result['rows']} rows"
        }
        
        if not validation_result["validation_passed"]:
            flow_result["message"] += f" (validation issues: {', '.join(validation_result['issues'])})"
            logger.warning(f"Table created but validation issues found: {validation_result['issues']}")
        else:
            logger.info("Processed AWaRe VMP mapping table created and validated successfully")

            stats = validation_result["stats"]
            if stats.get('vtm_mappings_applied', 0) > 0 or stats.get('vmp_mappings_applied', 0) > 0:
                logger.info(f"Historical mappings applied: {stats.get('vtm_mappings_applied', 0)} VTM mappings, {stats.get('vmp_mappings_applied', 0)} VMP mappings")
        
        return flow_result
        
    except Exception as e:
        logger.error(f"Error in processed AWaRe VMP mapping table creation flow: {str(e)}")



if __name__ == "__main__":
    create_aware_vmp_mapping()