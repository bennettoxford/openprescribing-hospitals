import requests
import pandas as pd
from datetime import datetime
from typing import Dict, Any
import time
from google.cloud import bigquery
from prefect import flow, task, get_run_logger

from pipeline.setup.bq_tables import SCMD_RAW_TABLE_SPEC, SCMD_DATA_STATUS_TABLE_SPEC
from pipeline.utils.utils import get_bigquery_client

@task
def check_data_exists_for_month(year_month_str: str) -> bool:
    """
    Check if data already exists for a given month in the SCMD raw table.
    
    Args:
        year_month_str: Year and month in format 'YYYYMM' (e.g., '201901')
    
    Returns:
        True if data exists, False otherwise
    """
    logger = get_run_logger()

    year_month_date = datetime.strptime(year_month_str, "%Y%m").date()
    
    client = get_bigquery_client()
    
    try:
        query = f"""
        SELECT COUNT(*) as record_count
        FROM `{SCMD_RAW_TABLE_SPEC.full_table_id}`
        WHERE year_month = '{year_month_date}'
        """
        
        logger.info(f"Checking if data exists for {year_month_str}")
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            record_count = row.record_count
            if record_count > 0:
                logger.info(f"Found {record_count:,} existing records for {year_month_str}")
                return True
            else:
                logger.info(f"No existing data found for {year_month_str}")
                return False
                
    except Exception as e:
        logger.info(f"Table doesn't exist or error checking data for {year_month_str}: {e}")
        return False
    
    return False

@task
def fetch_scmd_data_for_resource(resource_id: str) -> pd.DataFrame:
    """
    Fetch all SCMD data for a given resource ID from NHSBSA API with pagination.
    
    Args:
        resource_id: The resource ID (e.g., 'SCMD_201901')
    
    Returns:
        DataFrame containing all records for the resource
    """
    logger = get_run_logger()
    logger.info(f"Starting data fetch for {resource_id}")
    
    base_url = "https://opendata.nhsbsa.net/api/3/action/datastore_search"
    all_records = []
    offset = 0
    limit = 100
    last_logged_milestone = 0
    
    while True:
        params = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data.get("success", False):
                raise ValueError(f"API request failed for {resource_id}")
            
            result = data["result"]
            records = result.get("records", [])
            
            if not records:
                logger.info(f"No more records found for {resource_id}")
                break
                
            all_records.extend(records)

            current_total = len(all_records)
            if current_total - last_logged_milestone >= 10000:
                logger.info(f"Fetched {current_total:,} records so far for {resource_id}")
                last_logged_milestone = current_total
            
            total_records = result.get("total", 0)
            if offset + limit >= total_records:
                logger.info(f"Completed fetching data for {resource_id}. Total records: {len(all_records):,}")
                break
                
            offset += limit
            
            time.sleep(0.1)
            
        except requests.RequestException as e:
            logger.error(f"Error fetching data for {resource_id} at offset {offset}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error processing {resource_id}: {e}")
            raise
    
    if not all_records:
        logger.warning(f"No records found for {resource_id}")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_records)
    logger.info(f"Created DataFrame with {len(df):,} rows for {resource_id}")
    
    return df

@task
def transform_scmd_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform SCMD data to match BigQuery schema.
    
    Args:
        df: Raw DataFrame from API
    
    Returns:
        Transformed DataFrame matching SCMD_RAW_TABLE_SPEC schema
    """
    logger = get_run_logger()
    
    if df.empty:
        return df
    
    column_mapping = {
        "YEAR_MONTH": "year_month",
        "ODS_CODE": "ods_code", 
        "VMP_SNOMED_CODE": "vmp_snomed_code",
        "VMP_PRODUCT_NAME": "vmp_product_name",
        "UNIT_OF_MEASURE_IDENTIFIER": "unit_of_measure_identifier",
        "UNIT_OF_MEASURE_NAME": "unit_of_measure_name",
        "TOTAL_QUANITY_IN_VMP_UNIT": "total_quantity_in_vmp_unit"  # Note: API has typo "QUANITY"
    }
    
    df_transformed = df.rename(columns=column_mapping)
    
    df_transformed["year_month"] = pd.to_datetime(df_transformed["year_month"]).dt.date
    df_transformed["vmp_snomed_code"] = df_transformed["vmp_snomed_code"].astype(str)
    df_transformed["unit_of_measure_identifier"] = df_transformed["unit_of_measure_identifier"].astype(str)
    df_transformed["total_quantity_in_vmp_unit"] = df_transformed["total_quantity_in_vmp_unit"].astype(float)
    
    # Add indicative_cost column (not available in API, set to None)
    df_transformed["indicative_cost"] = None
    
    required_columns = [field.name for field in SCMD_RAW_TABLE_SPEC.schema]
    df_transformed = df_transformed[required_columns]
    
    logger.info(f"Transformed DataFrame shape: {df_transformed.shape}")
    
    return df_transformed

@task
def upload_to_bigquery(df: pd.DataFrame, table_spec: object) -> None:
    """
    Upload DataFrame to BigQuery table.
    
    Args:
        df: DataFrame to upload
        table_spec: Table specification object
    """
    logger = get_run_logger()
    
    if df.empty:
        logger.warning("DataFrame is empty, skipping upload")
        return
    
    client = get_bigquery_client()

    table_ref = client.dataset(table_spec.dataset_id).table(table_spec.table_id)
    
    try:
        table = client.get_table(table_ref)
        logger.info(f"Table {table_spec.full_table_id} already exists")
    except Exception:
        logger.info(f"Creating table {table_spec.full_table_id}")
        table = bigquery.Table(table_ref, schema=table_spec.schema)
        
        if table_spec.partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                field=table_spec.partition_field
            )
        
        if table_spec.cluster_fields:
            table.clustering_fields = table_spec.cluster_fields
            
        table.description = table_spec.description
        table = client.create_table(table)
        logger.info(f"Created table {table_spec.full_table_id}")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=table_spec.schema
    )
    
    logger.info(f"Uploading {len(df)} rows to {table_spec.full_table_id}")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  

    logger.info(f"Successfully uploaded {len(df)} rows to {table_spec.full_table_id}")

@task
def update_data_status(year_month_str: str, file_type: str = "finalised") -> None:
    """
    Update the SCMD data status table to mark a month as finalised.
    
    Args:
        year_month_str: Year and month in format 'YYYYMM' (e.g., '201901')
        file_type: Status to set (default: 'finalised')
    """
    logger = get_run_logger()
    
    year_month_date = datetime.strptime(year_month_str, "%Y%m").date()
    
    status_df = pd.DataFrame({
        "year_month": [year_month_date],
        "file_type": [file_type]
    })
    
    logger.info(f"Updating data status for {year_month_str} to '{file_type}'")
    
    upload_to_bigquery(status_df, SCMD_DATA_STATUS_TABLE_SPEC)

@task
def process_scmd_month(year_month: str) -> Dict[str, Any]:
    """
    Process SCMD data for a specific month.
    
    Args:
        year_month: Year and month in format 'YYYYMM' (e.g., '201901')
        
    Returns:
        Dict with processing results
    """
    logger = get_run_logger()
    logger.info(f"Processing SCMD data for {year_month}")
    
    if check_data_exists_for_month(year_month):
        logger.info(f"Data already exists for {year_month}, skipping import")
        return {"month": year_month, "status": "skipped", "reason": "data_exists"}
    
    resource_id = f"SCMD_{year_month}"
    logger.info(f"Beginning data fetch for month {year_month}")
    
    try:
        raw_df = fetch_scmd_data_for_resource(resource_id)
        
        if raw_df.empty:
            logger.warning(f"No data found for {year_month}")
            return {"month": year_month, "status": "failed", "reason": "no_data"}
        
        transformed_df = transform_scmd_data(raw_df)
        
        upload_to_bigquery(transformed_df, SCMD_RAW_TABLE_SPEC)
        
        update_data_status(year_month, "finalised")
        
        logger.info(f"Successfully processed {year_month} with {len(transformed_df):,} records")
        
        return {
            "month": year_month, 
            "status": "success", 
            "rows": len(transformed_df),
            "file_type": "finalised"
        }
        
    except Exception as e:
        logger.error(f"Error processing {year_month}: {e}")
        return {"month": year_month, "status": "failed", "reason": str(e)}

@flow(name="SCMD Pre-April 2019 Import")
def import_scmd_pre_apr_2019():
    """
    Import SCMD data for January, February, and March 2019.
    """
    logger = get_run_logger()
    
    months_to_process = ["201901", "201902", "201903"]
    
    logger.info(f"Will process {len(months_to_process)} months: {months_to_process}")
    
    results = []
    
    for i, year_month in enumerate(months_to_process, 1):
        logger.info(f"Starting month {i}/{len(months_to_process)}: {year_month}")
        result = process_scmd_month(year_month)
        results.append(result)
        logger.info(f"Completed month {i}/{len(months_to_process)}: {year_month}")
    
    successful_months = [r for r in results if r["status"] == "success"]
    skipped_months = [r for r in results if r["status"] == "skipped"] 
    failed_months = [r for r in results if r["status"] == "failed"]
    
    logger.info(f"Processing complete!")
    logger.info(f"Successful months ({len(successful_months)}): {[r['month'] for r in successful_months]}")
    if skipped_months:
        logger.info(f"Skipped months ({len(skipped_months)}): {[r['month'] for r in skipped_months]}")
    if failed_months:
        logger.warning(f"Failed months ({len(failed_months)}): {[r['month'] for r in failed_months]}")
    
    return {
        "total_months": len(months_to_process),
        "successful": len(successful_months),
        "skipped": len(skipped_months),
        "failed": len(failed_months),
        "results": results
    }

if __name__ == "__main__":
    import_scmd_pre_apr_2019()
