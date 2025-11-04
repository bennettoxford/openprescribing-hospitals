from io import StringIO
import re
import requests
from bs4 import BeautifulSoup
from typing import Optional
import pandas as pd
from prefect import flow, task, get_run_logger
from google.cloud import bigquery

from pipeline.setup.config import PROJECT_ID, DATASET_ID, ERIC_TRUST_DATA_TABLE_ID
from pipeline.utils.utils import get_bigquery_client

def extract_year_from_url(url):
            year_match = re.search(r'20(\d{2})[-/](\d{2})', url)
            if year_match:
                return int(year_match.group(1) + year_match.group(2))
            return 0

@task()
def find_latest_eric_data_url() -> Optional[tuple[str, str]]:
    """
    Find the latest ERIC trust data CSV URL from the NHS Digital ERIC pages.
    Returns a tuple of (URL, data_year) of the most recent trust data CSV file.
    """
    logger = get_run_logger()
    
    base_url = "https://digital.nhs.uk/data-and-information/publications/statistical/estates-returns-information-collection"
    
    try:
        logger.info(f"Fetching ERIC publications page: {base_url}")
        response = requests.get(base_url, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the past publications section
        past_publications_section = soup.find('div', {'id': 'past-publications'})
        if not past_publications_section:
            logger.warning("Could not find past publications section")
            return None
        
        # Find all publication links in the past publications section
        publication_links = []
        for link in past_publications_section.find_all('a', href=True):
            href = link['href']
            link_text = link.get_text(strip=True)
            
            # Look for "Summary page and dataset for ERIC" publications
            if ('summary-page-and-dataset-for-eric' in href and 
                'Summary page and dataset for ERIC' in link_text and
                re.search(r'20\d{2}[-/]\d{2}', href)):
                publication_links.append((href, link_text))
        
        if not publication_links:
            logger.warning("No ERIC summary publications found in past publications")
            return None

        
        publication_links.sort(key=lambda x: extract_year_from_url(x[0]), reverse=True)
        latest_publication_url, latest_publication_text = publication_links[0]
        
        # Extract year from the latest publication
        year_match = re.search(r'20(\d{2})[-/](\d{2})', latest_publication_url)
        if year_match:
            data_year = f"20{year_match.group(1)}_{year_match.group(2)}"
        else:
            logger.warning("Could not extract year from latest publication URL")
            return None
        
        if not latest_publication_url.startswith('http'):
            latest_publication_url = f"https://digital.nhs.uk{latest_publication_url}"
        
        logger.info(f"Found latest ERIC publication: {latest_publication_text} (year: {data_year})")
        logger.info(f"Publication URL: {latest_publication_url}")

        logger.info(f"Fetching publication page: {latest_publication_url}")
        pub_response = requests.get(latest_publication_url, timeout=30)
        pub_response.raise_for_status()
        pub_soup = BeautifulSoup(pub_response.content, 'html.parser')
        
        csv_pattern = lambda href: (
            href.endswith('.csv') and 
            'files.digital.nhs.uk' in href and 
            ('eric' in href.lower() or 'trust' in href.lower())
        )
        
        for link in pub_soup.find_all('a', href=True):
            href = link['href']
            if csv_pattern(href):
                return (href, data_year)
        
        logger.warning("No CSV data file found on the latest publication page")
        return None
        
    except Exception as e:
        logger.error(f"Error finding latest ERIC data URL: {e}")
        return None


@task()
def download_eric_data(csv_url: str) -> Optional[pd.DataFrame]:
    """
    Download and parse the ERIC trust data CSV.
    """
    logger = get_run_logger()
    
    try:
        logger.info(f"Downloading ERIC data from: {csv_url}")

        response = requests.get(csv_url, timeout=60)
        response.raise_for_status()
        
        logger.info(f"Downloaded {len(response.content)} bytes")
        
        try:
            
            csv_content = StringIO(response.text)
            df = pd.read_csv(csv_content)
            
            logger.info(f"Downloaded ERIC data with {len(df)} rows and {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Failed to parse CSV: {e}")
            raise
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error downloading ERIC data: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error downloading ERIC data: {e}")
        return None


@task()
def check_existing_eric_data(data_year: str) -> bool:
    """
    Check if ERIC data for the given year already exists in BigQuery for given year.
    Returns True if data exists, False otherwise.
    """
    logger = get_run_logger()
    
    try:
        client = get_bigquery_client()
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{ERIC_TRUST_DATA_TABLE_ID}"
        
        query = f"""
        SELECT COUNT(*) as count
        FROM `{table_id}`
        WHERE data_year = '{data_year}'
        """
        
        result = client.query(query).result()
        count = list(result)[0].count
        
        if count > 0:
            logger.info(f"ERIC data for year {data_year} already exists ({count} records)")
            return True
        else:
            logger.info(f"No existing ERIC data found for year {data_year}")
            return False
            
    except Exception as e:
        logger.error(f"Error checking existing ERIC data: {e}")
        return False


@task()
def clean_eric_data(df: pd.DataFrame, data_year: str) -> pd.DataFrame:
    """
    Clean and standardise the ERIC data for BigQuery upload.
    """
    logger = get_run_logger()
    
    try:
        logger.info(f"Starting data cleaning")
        
        cleaned_df = df.copy()
        cleaned_df['data_year'] = data_year

        # find the required columns - they contain the following
        columns_to_check = ['Trust Code', 'Trust Name', 'Trust Type']

        cols_to_keep = ["data_year"]

        # check if any of the columns contain the required columns
        for col in df.columns:
            if any(column_to_check in col for column_to_check in columns_to_check):
                cols_to_keep.append(col)

        filtered_df = cleaned_df[cols_to_keep]
        
        column_mapping = {}
        for col in filtered_df.columns:
            if 'Trust Code' in col:
                column_mapping[col] = "trust_code"
            elif 'Trust Name' in col:
                column_mapping[col] = "trust_name"
            elif 'Trust Type' in col:
                column_mapping[col] = "trust_type"
            elif col == 'data_year':
                column_mapping[col] = "data_year"
        
        filtered_df = filtered_df.rename(columns=column_mapping)
        
        # Convert trust_type to title case (e.g., "ACUTE - TEACHING" -> "Acute - Teaching")
        if 'trust_type' in filtered_df.columns:
            filtered_df['trust_type'] = filtered_df['trust_type'].apply(
                lambda x: x.title() if pd.notna(x) and isinstance(x, str) else x
            )
        
        # Remove rows where trust_code is empty or null
        initial_rows = len(filtered_df)
        filtered_df = filtered_df.dropna(subset=['trust_code'])
        filtered_df = filtered_df[filtered_df['trust_code'] != '']
        filtered_df = filtered_df[filtered_df['trust_code'].str.strip() != '']
        
        removed_rows = initial_rows - len(filtered_df)
        if removed_rows > 0:
            logger.info(f"Removed {removed_rows} rows with missing trust codes")
 
        required_columns = ['trust_code', 'trust_name', 'trust_type', 'data_year']
        
        available_columns = [col for col in required_columns if col in filtered_df.columns]
        missing_columns = [col for col in required_columns if col not in filtered_df.columns]
        
        if missing_columns:
            logger.warning(f"Missing required columns for BigQuery schema: {missing_columns}")

        final_df = filtered_df[available_columns].copy()
        
        return final_df
        
    except Exception as e:
        logger.error(f"Error cleaning ERIC data: {e}")
        raise


@task()
def upload_eric_to_bigquery(df: pd.DataFrame) -> bool:
    """
    Upload the cleaned ERIC data to BigQuery.
    """
    logger = get_run_logger()
    
    try:
        if df is None or len(df) == 0:
            logger.error("No data to upload to BigQuery")
            return False
            
        client = get_bigquery_client()
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{ERIC_TRUST_DATA_TABLE_ID}"

        data_year = df['data_year'].iloc[0] if len(df) > 0 else None
        logger.info(f"Preparing to upload {len(df)} records for year {data_year}")
        
        # Clear existing data for the same year
        if data_year:
            try:
                delete_query = f"""
                DELETE FROM `{table_id}` 
                WHERE data_year = '{data_year}'
                """
                logger.info(f"Clearing existing data for year {data_year}")
                delete_job = client.query(delete_query)
                delete_job.result()
                logger.info("Successfully cleared existing data")
            except Exception as e:
                logger.warning(f"Could not clear existing data (table might not exist): {e}")
        
        upload_df = df.copy()
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            autodetect=False
        )
        
        logger.info(f"Starting upload to {table_id}")
        job = client.load_table_from_dataframe(upload_df, table_id, job_config=job_config)
        
        try:
            job.result(timeout=60)
        except Exception as e:
            logger.error(f"Upload job timed out or failed: {e}")
            return False
        
        if job.state == 'DONE':
            if job.errors:
                logger.error(f"Upload job completed with errors: {job.errors}")
                return False
            else:
                logger.info(f"Successfully uploaded {len(upload_df)} ERIC records to {table_id}")
                                
                return True
        else:
            logger.error(f"Upload job did not complete successfully. State: {job.state}")
            return False
        
    except Exception as e:
        logger.error(f"Error uploading ERIC data to BigQuery: {e}")
        return False


@flow(name="import_eric_data")
def import_eric_data():
    """
    Main flow to import ERIC trust data from NHS Digital.
    """
    logger = get_run_logger()
    logger.info("Starting ERIC data import")
    
    try:
        logger.info("Step 1: Finding latest ERIC data URL")
        result = find_latest_eric_data_url()
        if not result:
            logger.error("Could not find latest ERIC data URL")
            return False
        
        csv_url, data_year = result
        logger.info(f"Found data URL for year {data_year}: {csv_url}")
        
        logger.info("Step 2: Checking for existing data")
        if check_existing_eric_data(data_year):
            logger.info(f"ERIC data for year {data_year} already exists. Skipping import.")
            return True
        
        logger.info("Step 3: Downloading ERIC data")
        df = download_eric_data(csv_url)
        if df is None or len(df) == 0:
            logger.error("Failed to download ERIC data or data is empty")
            return False
        
        logger.info(f"Downloaded data: {len(df)} rows, {len(df.columns)} columns")
        
        
        logger.info("Step 4: Cleaning ERIC data")
        cleaned_df = clean_eric_data(df, data_year)
        
        if cleaned_df is None or len(cleaned_df) == 0:
            logger.error("Data cleaning failed or resulted in empty dataset")
            return False
        
        
        if 'trust_code' not in cleaned_df.columns:
            logger.error("Cleaned data missing required 'trust_code' column")
            return False
        
        
        logger.info("Step 5: Uploading to BigQuery")
        success = upload_eric_to_bigquery(cleaned_df)
        
        if success:
            logger.info("ERIC data import completed successfully")
            logger.info(f"Final summary: {len(cleaned_df)} records uploaded for year {data_year}")
        else:
            logger.error("ERIC data import failed during upload")
            
        return success
        
    except Exception as e:
        logger.error(f"Error in ERIC data import flow: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False


if __name__ == "__main__":
    import_eric_data()
