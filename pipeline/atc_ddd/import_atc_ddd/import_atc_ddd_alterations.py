import pandas as pd
import requests
from prefect import task, flow
from prefect.logging import get_run_logger
from google.cloud import bigquery
from io import BytesIO

from pipeline.utils.utils import get_bigquery_client
from pipeline.setup.bq_tables import (
    WHO_DDD_ALTERATIONS_TABLE_SPEC,
    WHO_ATC_ALTERATIONS_TABLE_SPEC,
)

YEARLY_UPDATES_URL = "https://atcddd.fhi.no/filearchive/documents/atc_ddd_new_and_alterations_2025_final.xlsx"


@task
def load_to_bigquery(df: pd.DataFrame, table_spec):
    """Load DataFrame to BigQuery"""
    logger = get_run_logger()
    logger.info(f"Loading data to {table_spec.full_table_id}")

    client = get_bigquery_client()

    job_config = bigquery.LoadJobConfig(
        schema=table_spec.schema,
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        df, table_spec.full_table_id, job_config=job_config
    )
    job.result()

    logger.info(f"Loaded {len(df)} rows to {table_spec.full_table_id}")


@task
def fetch_excel(url: str = YEARLY_UPDATES_URL) -> tuple[dict, int]:
    """Download and read Excel file, and extract implementation year from URL"""
    logger = get_run_logger()
    logger.info(f"Downloading Excel file from {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to download Excel file from {url}. This is required for processing yearly updates. Error: {str(e)}")
    
    try:
        year = int(url.split('_')[-2])
        
        with BytesIO(response.content) as excel_data:
            dfs = pd.read_excel(excel_data, sheet_name=None)
            return dfs, year
            
    except ValueError as e:
        raise RuntimeError(f"Failed to extract year from URL {url}. Expected format: atc_ddd_new_and_alterations_YYYY_final.xlsx")
    except Exception as e:
        raise RuntimeError(f"Failed to parse Excel file. Error: {str(e)}")


@task
def process_new_atc_codes(dfs: dict, implementation_year: int) -> pd.DataFrame:
    """Process new ATC codes from the Excel file and format them to match ATC alterations table"""
    logger = get_run_logger()
    
    new_atc_df = dfs.get('New ATC 5th levels', pd.DataFrame())
    
    if new_atc_df.empty:
        logger.warning("No new ATC codes found in Excel file")
        return pd.DataFrame()
    
    processed_df = pd.DataFrame({
        'substance': new_atc_df['Substance name'],
        'previous_atc_code': None,
        'new_atc_code': new_atc_df['New ATC code'],
        'year_changed': implementation_year,
        'comment': 'New code'
    })
    
    logger.info(f"Processed {len(processed_df)} new ATC codes")
    return processed_df


@task
def process_new_ddds(dfs: dict, implementation_year: int) -> pd.DataFrame:
    """Process new DDDs from the Excel file and format them to match DDD alterations table"""
    logger = get_run_logger()
    
    new_ddd_df = dfs.get('New DDDs', pd.DataFrame())
    
    if new_ddd_df.empty:
        logger.warning("No new DDDs found in Excel file")
        return pd.DataFrame()

    processed_df = pd.DataFrame({
        'substance': new_ddd_df['ATC level name'],
        'previous_ddd': None,
        'previous_ddd_unit': None,
        'previous_route': None,
        'new_ddd': new_ddd_df['New DDD'].astype(float),
        'new_ddd_unit': new_ddd_df['Unit'],
        'new_route': new_ddd_df['Adm.route'],
        'atc_code': new_ddd_df['ATC code'],
        'year_changed': implementation_year,
        'comment': new_ddd_df['Note'].fillna('New DDD')
    })
    
    logger.info(f"Processed {len(processed_df)} new DDDs")
    return processed_df


@task
def process_new_atc_34_levels(dfs: dict, implementation_year: int) -> pd.DataFrame:
    """Process new 3rd and 4th level ATC codes from the Excel file and format them to match ATC alterations table"""
    logger = get_run_logger()
    
    new_atc_34_df = dfs.get('New 3rd and 4th levels', pd.DataFrame())
    
    if new_atc_34_df.empty:
        logger.warning("No new 3rd and 4th level ATC codes found in Excel file")
        return pd.DataFrame()
    
    processed_df = pd.DataFrame({
        'substance': new_atc_34_df['New ATC level name'],
        'previous_atc_code': None,
        'new_atc_code': new_atc_34_df['ATC code'],
        'year_changed': implementation_year,
        'comment': 'New 3rd/4th level code'
    })
    
    logger.info(f"Processed {len(processed_df)} new 3rd and 4th level ATC codes")
    return processed_df


@task
def process_atc_name_alterations(dfs: dict, implementation_year: int) -> pd.DataFrame:
    """Process ATC level name alterations from the Excel file and format them to match ATC alterations table"""
    logger = get_run_logger()

    atc_name_df = dfs.get('ATC level name alterations', pd.DataFrame())
    
    if atc_name_df.empty:
        logger.warning("No ATC level name alterations found in Excel file")
        return pd.DataFrame()
    
    processed_df = pd.DataFrame({
        'substance': atc_name_df['New ATC level name'],
        'previous_atc_code': atc_name_df['ATC code'],
        'new_atc_code': atc_name_df['ATC code'],
        'year_changed': implementation_year,
        'comment': 'Name updated (code unchanged): ' + atc_name_df['Previous ATC level name'] + ' → ' + atc_name_df['New ATC level name']
    })
    
    logger.info(f"Processed {len(processed_df)} ATC level name alterations")
    return processed_df


@flow(name="Import ATC DDD Alterations")
def import_atc_ddd_alterations():
    """Main flow to import ATC and DDD alterations into BigQuery.
    
    This processes data from the yearly updates Excel file which includes:
    - New ATC codes at the 5th level
    - New ATC codes at 3rd and 4th levels
    - Changes to ATC level names (where the code remains the same)
    - New DDD assignments for substances
    """
    logger = get_run_logger()
    logger.info("Starting ATC DDD alterations import flow")

    excel_dfs, implementation_year = fetch_excel()

    # Process new DDDs
    ddd_df = process_new_ddds(excel_dfs, implementation_year)
    if not ddd_df.empty:
        ddd_df = ddd_df.dropna(axis=1, how='all')
    
    if not ddd_df.empty:
        load_to_bigquery(ddd_df, WHO_DDD_ALTERATIONS_TABLE_SPEC)
    else:
        logger.warning("No DDD data to load")

    # Process ATC codes - combine all sources
    atc_dfs = []
    
    # Get new 5th level codes
    new_atc_df = process_new_atc_codes(excel_dfs, implementation_year)
    if not new_atc_df.empty:
        new_atc_df = new_atc_df.dropna(axis=1, how='all')
        if not new_atc_df.empty:
            atc_dfs.append(new_atc_df)
    
    # Get new 3rd and 4th level codes
    new_atc_34_df = process_new_atc_34_levels(excel_dfs, implementation_year)
    if not new_atc_34_df.empty:
        new_atc_34_df = new_atc_34_df.dropna(axis=1, how='all')
        if not new_atc_34_df.empty:
            atc_dfs.append(new_atc_34_df)
    
    # Get ATC level name alterations
    atc_name_df = process_atc_name_alterations(excel_dfs, implementation_year)
    if not atc_name_df.empty:
        atc_name_df = atc_name_df.dropna(axis=1, how='all')
        if not atc_name_df.empty:
            atc_dfs.append(atc_name_df)
    
    if atc_dfs:
        atc_df = pd.concat(atc_dfs, ignore_index=True)
        load_to_bigquery(atc_df, WHO_ATC_ALTERATIONS_TABLE_SPEC)
    else:
        logger.warning("No ATC data to load")

    logger.info("Completed ATC DDD alterations import flow")

if __name__ == "__main__":
    import_atc_ddd_alterations()