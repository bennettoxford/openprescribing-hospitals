import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefect import task, flow
from prefect.logging import get_run_logger
from google.cloud import bigquery
from io import BytesIO

from pipeline.utils.utils import get_bigquery_client
from pipeline.setup.bq_tables import (
    WHO_DDD_ALTERATIONS_TABLE_SPEC,
    WHO_ATC_ALTERATIONS_TABLE_SPEC,
)

DDD_ALTERATIONS_URL = "https://atcddd.fhi.no/atc_ddd_alterations__cumulative/ddd_alterations/"
ATC_ALTERATIONS_URL = "https://atcddd.fhi.no/atc_ddd_alterations__cumulative/atc_alterations/"
YEARLY_UPDATES_URL = "https://atcddd.fhi.no/filearchive/documents/atc_ddd_new_and_alterations_2025_final.xlsx"


@task
def fetch_html(url: str) -> str:
    """Fetch HTML content from URL"""
    logger = get_run_logger()
    logger.info(f"Fetching data from {url}")
    
    response = requests.get(url)
    response.raise_for_status()
    return response.text


@task
def parse_ddd_alterations(html: str) -> pd.DataFrame:
    """Parse DDD alterations from HTML table"""
    logger = get_run_logger()
    logger.info("Parsing DDD alterations table")
    
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    
    if not table:
        raise ValueError("Could not find DDD alterations table in HTML")
    
    rows = []
    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        if len(cols) >= 8:
            comments = set()
            for col in cols:
                sup = col.find('sup')
                if sup:
                    a_tag = sup.find('a')
                    if a_tag and 'title' in a_tag.attrs:
                        comments.add(a_tag['title'].strip())
            
            comment = '; '.join(sorted(comments)) if comments else None
            
            for col in cols:
                sup = col.find('sup')
                if sup:
                    sup.decompose()
            
            substance = cols[0].get_text(strip=True)

            prev_ddd_text = cols[1].get_text(strip=True)
            prev_ddd_unit = cols[2].get_text(strip=True)
            try:
                prev_ddd = float(prev_ddd_text) if prev_ddd_text else None
            except ValueError:
                prev_ddd = None
            
            prev_route = cols[3].get_text(strip=True)

            new_ddd_text = cols[4].get_text(strip=True)
            new_ddd_unit = cols[5].get_text(strip=True)
            try:
                new_ddd = float(new_ddd_text) if new_ddd_text else None
            except ValueError:
                new_ddd = None
            
            new_route = cols[6].get_text(strip=True)
            
            atc_code = cols[7].get_text(strip=True)
            
            year_text = cols[8].get_text(strip=True) if len(cols) > 8 else None
            try:
                year = int(year_text) if year_text else None
            except ValueError:
                year = None
            
            rows.append({
                'substance': substance,
                'previous_ddd': prev_ddd,
                'previous_ddd_unit': prev_ddd_unit,
                'previous_route': prev_route,
                'new_ddd': new_ddd,
                'new_ddd_unit': new_ddd_unit,
                'new_route': new_route,
                'atc_code': atc_code,
                'year_changed': year,
                'comment': comment
            })
    
    df = pd.DataFrame(rows)
    logger.info(f"Parsed {len(df)} DDD alterations")
    return df

def clean_atc(text):
    result = text.split("(existing code)")[0].strip()
    return result if result else None

@task
def parse_atc_alterations(html: str) -> pd.DataFrame:
    """Parse ATC alterations from HTML table"""
    logger = get_run_logger()
    logger.info("Parsing ATC alterations table")
    
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    
    if not table:
        raise ValueError("Could not find ATC alterations table in HTML")
    
    rows = []
    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        if len(cols) >= 4:

            comments = set()
            
            for col in cols:
                sup = col.find('sup')
                if sup:
                    a_tag = sup.find('a')
                    if a_tag and 'title' in a_tag.attrs:
                        comments.add(a_tag['title'].strip())
                    sup.decompose()
                    
            comment = '; '.join(sorted(comments)) if comments else None

            prev_atc_text = cols[0].get_text(strip=True)
            prev_atc = clean_atc(prev_atc_text)
            
            substance = cols[1].get_text(strip=True).strip()
            
            new_atc_text = cols[2].get_text(strip=True)
            new_atc = clean_atc(new_atc_text)
            
            year_text = cols[3].get_text(strip=True).strip()
            try:
                year = int(year_text) if year_text else None
            except ValueError:
                year = None
            
            if prev_atc and new_atc:
                rows.append({
                    'substance': substance,
                    'previous_atc_code': prev_atc,
                    'new_atc_code': new_atc,
                    'year_changed': year,
                    'comment': comment
                })
    
    df = pd.DataFrame(rows)
    
    logger.info(f"Parsed {len(df)} ATC alterations")
    return df


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
    
    This processes data from two complementary sources to maintain an up-to-date
    record of all changes to the ATC/DDD system:

    1. Cumulative alterations lists:
       - These are HTML tables containing the historical record of all:
         * ATC code changes
         * DDD value changes
       - They do not include certain types of updates (new codes, ATC name changes not associated with a code change)
    
    2. Yearly updates Excel file:
       - Includes several types of updates not found in the cumulative lists:
         * New ATC codes at the 5th level
         * New ATC codes at 3rd and 4th levels
         * Changes to ATC level names (where the code remains the same)
         * New DDD assignments for substances previously without a DDD
    
    This first processes the cumulative lists and then supplements with the latest updates
    from the Excel file
    """
    logger = get_run_logger()
    logger.info("Starting ATC DDD alterations import flow")

    excel_dfs, implementation_year = fetch_excel()

    # Process DDD alterations and new DDDs
    ddd_html = fetch_html(DDD_ALTERATIONS_URL)
    ddd_df = parse_ddd_alterations(ddd_html)
    
    # Get new DDDs and combine with alterations
    new_ddd_df = process_new_ddds(excel_dfs, implementation_year)
    if not new_ddd_df.empty:
        new_ddd_df = new_ddd_df.dropna(axis=1, how='all')

        if not new_ddd_df.empty:
            ddd_df = pd.concat([ddd_df, new_ddd_df], ignore_index=True)
    
    load_to_bigquery(ddd_df, WHO_DDD_ALTERATIONS_TABLE_SPEC)

    # Process ATC alterations and new codes
    atc_html = fetch_html(ATC_ALTERATIONS_URL)
    atc_df = parse_atc_alterations(atc_html)
    
    # Get new 5th level codes
    new_atc_df = process_new_atc_codes(excel_dfs, implementation_year)
    
    # Get new 3rd and 4th level codes
    new_atc_34_df = process_new_atc_34_levels(excel_dfs, implementation_year)
    
    # Get ATC level name alterations
    atc_name_df = process_atc_name_alterations(excel_dfs, implementation_year)
    
    if not new_atc_df.empty:
        new_atc_df = new_atc_df.dropna(axis=1, how='all')
        if not new_atc_df.empty:
            atc_df = pd.concat([atc_df, new_atc_df], ignore_index=True)
    if not new_atc_34_df.empty:
        new_atc_34_df = new_atc_34_df.dropna(axis=1, how='all')
        if not new_atc_34_df.empty:
            atc_df = pd.concat([atc_df, new_atc_34_df], ignore_index=True)
    if not atc_name_df.empty:
        atc_name_df = atc_name_df.dropna(axis=1, how='all')
        if not atc_name_df.empty:
            atc_df = pd.concat([atc_df, atc_name_df], ignore_index=True)
    
    load_to_bigquery(atc_df, WHO_ATC_ALTERATIONS_TABLE_SPEC)

    logger.info("Completed ATC DDD alterations import flow")

if __name__ == "__main__":
    import_atc_ddd_alterations()