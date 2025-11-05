import requests
import pandas as pd
import io
from datetime import datetime
from typing import Dict, Any, Optional
from google.cloud import bigquery
from prefect import flow, task, get_run_logger

from pipeline.setup.bq_tables import SCMD_RAW_FINALISED_TABLE_SPEC
from pipeline.utils.utils import get_bigquery_client


@task
def check_data_exists_for_month(year_month_str: str) -> bool:
    """
    Check if data already exists for a given month in the SCMD raw
    finalised table.
    
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
        FROM `{SCMD_RAW_FINALISED_TABLE_SPEC.full_table_id}`
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
def get_csv_download_url_for_month(year_month_str: str) -> Optional[str]:
    """
    Get the CSV download URL for a specific month from the retired SCMD dataset.
    
    Args:
        year_month_str: Year and month in format 'YYYYMM' (e.g., '201901')
    
    Returns:
        CSV download URL if found, None otherwise
    """
    logger = get_run_logger()
    logger.info(f"Getting CSV download URL for {year_month_str}")
    
    api_url = "https://opendata.nhsbsa.net/api/3/action/package_show?id=secondary-care-medicines-data"
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        
        if not data.get("success", False):
            raise ValueError("API request failed")
        
        resources = data["result"]["resources"]
        
        target_name = f"SCMD_{year_month_str}"
        
        for resource in resources:
            if resource.get("name") == target_name and resource.get("format") == "CSV":
                url = resource.get("url")
                if url:
                    logger.info(f"Found CSV download URL for {year_month_str}: {url}")
                    return url
        
        logger.warning(f"No CSV resource found for {year_month_str}")
        return None
        
    except requests.RequestException as e:
        logger.error(f"Error fetching package info for {year_month_str}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error getting URL for {year_month_str}: {e}")
        raise


@task
def download_csv_data(url: str, year_month_str: str) -> pd.DataFrame:
    """
    Download and parse CSV data from the given URL.
    
    Args:
        url: CSV download URL
        year_month_str: Year and month for logging purposes
    
    Returns:
        DataFrame containing the CSV data
    """
    logger = get_run_logger()
    logger.info(f"Downloading CSV data for {year_month_str} from {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()

        df = pd.read_csv(
            io.StringIO(response.text),
            dtype={
                "YEAR_MONTH": "str",
                "ODS_CODE": "str", 
                "VMP_SNOMED_CODE": "str",
                "VMP_PRODUCT_NAME": "str",
                "UNIT_OF_MEASURE_IDENTIFIER": "str",
                "UNIT_OF_MEASURE_NAME": "str",
                "TOTAL_QUANITY_IN_VMP_UNIT": "float64"  # Note: API has typo "QUANITY"
            }
        )
        
        logger.info(f"Successfully downloaded {len(df):,} rows for {year_month_str}")
        return df
        
    except requests.RequestException as e:
        logger.error(f"Error downloading CSV for {year_month_str}: {e}")
        raise
    except pd.errors.EmptyDataError as e:
        logger.error(f"Empty or invalid CSV data for {year_month_str}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing CSV for {year_month_str}: {e}")
        raise


@task
def transform_scmd_data(df: pd.DataFrame, year_month_str: str) -> pd.DataFrame:
    """
    Transform SCMD data to match BigQuery schema.
    
    Args:
        df: Raw DataFrame from CSV
        year_month_str: Year and month in format 'YYYYMM' for date conversion
    
    Returns:
        Transformed DataFrame matching SCMD_RAW_FINALISED_TABLE_SPEC schema
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
    
    year_month_date = datetime.strptime(year_month_str, "%Y%m").date()
    df_transformed["year_month"] = year_month_date

    df_transformed["vmp_snomed_code"] = df_transformed["vmp_snomed_code"].astype(str)
    df_transformed["unit_of_measure_identifier"] = df_transformed["unit_of_measure_identifier"].astype(str)
    df_transformed["total_quantity_in_vmp_unit"] = df_transformed["total_quantity_in_vmp_unit"].astype(float)
    df_transformed["unit_of_measure_name"] = df_transformed["unit_of_measure_name"].str.lower()
    df_transformed["indicative_cost"] = None
    
    required_columns = [field.name for field in SCMD_RAW_FINALISED_TABLE_SPEC.schema]
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

    logger.info(
        f"Successfully uploaded {len(df)} rows to "
        f"{table_spec.full_table_id}"
    )


@task
def process_scmd_month(year_month: str) -> Dict[str, Any]:
    """
    Process SCMD data for a specific month using CSV download.
    
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
    
    try:
        csv_url = get_csv_download_url_for_month(year_month)
        
        if not csv_url:
            logger.warning(f"No CSV download URL found for {year_month}")
            return {"month": year_month, "status": "failed", "reason": "no_csv_url"}
        
        raw_df = download_csv_data(csv_url, year_month)
        
        if raw_df.empty:
            logger.warning(f"No data found in CSV for {year_month}")
            return {"month": year_month, "status": "failed", "reason": "no_data"}
        
        transformed_df = transform_scmd_data(raw_df, year_month)
        
        upload_to_bigquery(transformed_df, SCMD_RAW_FINALISED_TABLE_SPEC)
        
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
    
    logger.info("Processing complete!")
    logger.info(
        f"Successful months ({len(successful_months)}): "
        f"{[r['month'] for r in successful_months]}"
    )
    if skipped_months:
        logger.info(
            f"Skipped months ({len(skipped_months)}): "
            f"{[r['month'] for r in skipped_months]}"
        )
    if failed_months:
        logger.warning(
            f"Failed months ({len(failed_months)}): "
            f"{[r['month'] for r in failed_months]}"
        )

if __name__ == "__main__":
    import_scmd_pre_apr_2019()
