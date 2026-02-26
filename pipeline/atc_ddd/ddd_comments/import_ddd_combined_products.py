import re
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Union
import pandas as pd
from prefect import task, flow
from prefect.logging import get_run_logger
from google.cloud import bigquery, storage

from pipeline.utils.utils import (
    get_bigquery_client,
    download_from_gcs,
    cleanup_temp_files
)
from pipeline.setup.config import BUCKET_NAME
from pipeline.setup.bq_tables import WHO_DDD_COMBINED_PRODUCTS_TABLE_SPEC

GCS_BASE_PATH = "who_atc_ddd_op_hosp"
FILE_NAME = "list_of_ddds_combined_products_2025.csv"
TEMP_DIR = Path("temp/ddd_combined_products")

# There are a range of different dosage forms. This maps them to the dm+d standard dosage forms
# wildcard are used where the WHO dosage form could map to multiple dm+d dosage forms
DOSAGE_FORM_MAPPING = {
    'caps': {
        'form': 'capsule',
        'route': 'oral',
    },
    'powder, single dose sachets': {
        'form': 'powder',
        'route': 'oral',
    },
    'oral paste': {
        'form': 'paste',
        'route': 'oral',
    },
    'concentrate for oral solution': {
        'form': 'solution',
        'route': 'oral',
    },
    'chew.tab': {
        'form': 'tabletchewable',
        'route': 'oral',
    },
    'tab': {
        'form': 'tablet',
        'route': 'oral',
    },
    'enterocaps': {
        'form': 'capsulegastro-resistant',
        'route': 'oral',
    },
    'enterotab': {
        'form': 'tabletgastro-resistant',
        'route': 'oral',
    },
    'eff granules': {
        'form': 'granules',
        'route': 'oral',
    },
    'eff tab': {
        'form': 'solution',
        'route': 'oral',
    },
    'prolonged-release granules, single dose sachets': {
        'form': 'granulesgastro-resistantmodified-release',
        'route': 'oral',
    },
    'inj': {
        'form': '*injection',
        'route': '*',
    },
    'inj sol': {
        'form': 'solutioninjection',
        'route': '*',
    },
    'powder for inj': {
        'form': 'solutioninjection',
        'route': '*',
    },
    'powder for injection': {
        'form': 'solutioninjection',
        'route': '*',
    },
    'inf conc': {
        'form': '*infusion',
        'route': '*',
    },
    'inf sol': {
        'form': 'solutioninfusion',
        'route': '*',
    },
    'mixt': {
        'form': '*',
        'route': '*',
    },
    'granules, single dose sachets': {
        'form': 'granules',
        'route': 'oral',
    },
    'supp': {
        'form': 'suppository',
        'route': '*',
    },
    'oromucosal spray': {
        'form': '*spray',
        'route': 'oralmucosal',
    },
    'oral sol': {
        'form': 'solution',
        'route': 'oral',
    },
    'nasal spray': {
        'form': '*spray',
        'route': 'nasal',
    },
    'inhal sol': {
        'form': 'inhalationsolution',
        'route': 'inhalation',
    },
    'inhal powd': {
        'form': 'powderinhalation',
        'route': 'inhalation',
    },
    'inhal aer': {
        'form': '*',
        'route': 'inhalation',
    },
    'inhal powd turbohaler': {
        'form': 'powderinhalation',
        'route': 'inhalation',
    },
    'inhal powd Turbohaler': {
        'form': 'powderinhalation',
        'route': 'inhalation',
    },
    'modified-release tab': {
        'form': 'tabletmodified-release',
        'route': 'oral',
    },
}


# Mapping from WHO combined products units to dm+d standard units
UNIT_MAPPING = {
    'caps': 'capsule',
    'sachets': 'sachet',
    'sachet': 'sachet',
    'ml': 'ml',
    'tab': 'tablet',
    'ampoulle': 'ampoule',
    'grams': 'gram',
    'vials': 'vial',
    'supp': 'suppository',
    'sprays': 'dose',
    'doses': 'dose',
    'inhal powd': 'dose',
}


@task
def fetch_csv_from_gcs() -> pd.DataFrame:
    """Find, download, and read the combined products CSV file from GCS"""
    logger = get_run_logger()
    logger.info(f"Finding combined products file in {BUCKET_NAME}/{GCS_BASE_PATH}")

    bq_client = get_bigquery_client()
    storage_client = storage.Client(
        project=bq_client.project,
        credentials=bq_client._credentials
    )

    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=GCS_BASE_PATH)

    gcs_path = None
    for blob in blobs:
        if FILE_NAME in blob.name:
            logger.info(f"Found file: {blob.name}")
            gcs_path = blob.name
            break

    if gcs_path is None:
        raise FileNotFoundError(
            f"Could not find {FILE_NAME} in {BUCKET_NAME}/{GCS_BASE_PATH}"
        )

    logger.info(f"Fetching {gcs_path} from GCS")
    file_path = download_from_gcs(BUCKET_NAME, gcs_path, TEMP_DIR)
    
    df = pd.read_csv(
        file_path,
        sep='\t',
        quotechar='"',
        encoding='latin-1',
        on_bad_lines='skip'
    )
    
    logger.info(f"Read {len(df)} rows from CSV")
    return df


def extract_ud_value(ddd_combined: str) -> Optional[float]:
    """Extract the numeric UD value from the DDD combined string.
    
    Examples:
        "2 UD (=2 tab)" -> 2.0
        "1.5 UD (=1.5 supp)" -> 1.5
        "12 UD (=12 caps)" -> 12.0
    """
    if pd.isna(ddd_combined) or not ddd_combined:
        return None
    
    # Match pattern like "2 UD" or "1.5 UD" at the start
    match = re.match(r'^(\d+\.?\d*)\s*UD', str(ddd_combined).strip())
    if match:
        return float(match.group(1))
    
    return None


def extract_ingredients_quantity_unit(active_ingredients: str) -> List[Dict[str, Optional[Union[float, str]]]]:
    """Extract ingredients, quantities, and units from the active ingredients per unit dose column.
    
    The format is ingredients separated by '/' with each ingredient having (denominator not always present):
    <ingredient name> <numerator quantity> <numerator unit>[/ <denominator quantity> <denominator unit>]
    
    Args:
        active_ingredients: String containing active ingredients per unit dose
        
    Returns:
        List of dictionaries with ingredients and their quantities
    """
    if pd.isna(active_ingredients) or not active_ingredients:
        return []
    
    ingredients_list = []
    ingredients_str = str(active_ingredients).strip()
    
    # If there's no '/' in the string, include the string as-is - this refers to groups of ingredients, e.g. multienzymes (lipase, protease etc.)
    if '/' not in ingredients_str:
        ingredient_name = ingredients_str
        # Strip "Comb. of " prefix from ingredient name (e.g., "Comb. of benzylpenicillin") - this is present before some sets of ingredients
        if ingredient_name.startswith("Comb. of "):
            ingredient_name = ingredient_name[len("Comb. of "):].strip()
        return [{
            'ingredient': ingredient_name,
            'numerator_quantity': None,
            'numerator_unit': None,
            'denominator_quantity': None,
            'denominator_unit': None
        }]
    
    # Split by '/' to get parts
    parts = [part.strip() for part in ingredients_str.split('/') if part.strip()]
    
    # Pattern to match ingredient with quantity and unit: <ingredient name> <quantity> <unit> [optional text]
    # Allows for text after the unit like "(metered dose)" or "(delivered dose)"
    ingredient_pattern = r'^(.+?)\s+(\d+\.?\d*)\s+([a-zA-Z]+)(?:\s+.*)?$'
    
    # Pattern to match just quantity and unit (denominator): <quantity> <unit>
    denominator_pattern = r'^(\d+\.?\d*)\s+([a-zA-Z]+)$'
    
    i = 0
    while i < len(parts):
        part = parts[i]
        
        # Check if this is a denominator (just number and unit, no ingredient name)
        denominator_match = re.match(denominator_pattern, part)
        if denominator_match and ingredients_list:
            # This is a denominator for the previous ingredient
            denominator_quantity = float(denominator_match.group(1))
            denominator_unit = denominator_match.group(2).strip()
   
            ingredients_list[-1]['denominator_quantity'] = denominator_quantity
            ingredients_list[-1]['denominator_unit'] = denominator_unit
            i += 1
            continue
        
        # Check if this is an ingredient with quantity and unit
        ingredient_match = re.match(ingredient_pattern, part)
        if ingredient_match:
            ingredient_name = ingredient_match.group(1).strip()

            if ingredient_name.startswith("Comb. of "):
                ingredient_name = ingredient_name[len("Comb. of "):].strip()
            numerator_quantity = float(ingredient_match.group(2))
            numerator_unit = ingredient_match.group(3).strip()
            
            ingredients_list.append({
                'ingredient': ingredient_name,
                'numerator_quantity': numerator_quantity,
                'numerator_unit': numerator_unit,
                'denominator_quantity': None,
                'denominator_unit': None
            })
        else:
            # If pattern doesn't match, include the ingredient anyway with null quantities
            ingredient_name = part.strip()
            if ingredient_name.startswith("Comb. of "):
                ingredient_name = ingredient_name[len("Comb. of "):].strip()
            ingredients_list.append({
                'ingredient': ingredient_name,
                'numerator_quantity': None,
                'numerator_unit': None,
                'denominator_quantity': None,
                'denominator_unit': None
            })
        
        i += 1
    
    return ingredients_list


def extract_converted_value_and_unit(ddd_combined: str) -> Tuple[Optional[float], Optional[str]]:
    """Extract the value and unit from DDD combined string and convert to dm+d unit.
    
    Example:
        "2 UD (=2 tab)" -> (2.0, 'tablet')
    
    Returns:
        tuple: (value, converted_unit) or (None, None) if no match
    """
    if pd.isna(ddd_combined) or not ddd_combined:
        return None, None
    
    ddd_str = str(ddd_combined).strip()
    
    # Pattern 1: Standard format "=<number> <unit>" or "= <number> <unit>"
    # e.g., "(=2 tab)", "(= 4 doses inhal aer)"
    match = re.search(r'\(\s*=\s*(\d+\.?\d*)\s+(.+?)\s*\)', ddd_str)
    
    # Pattern 2: "defined as" format
    # e.g., "(defined as 2 vials)"
    if not match:
        match = re.search(r'\(\s*defined as\s+(\d+\.?\d*)\s+(.+?)\s*\)', ddd_str)
    
    if match:
        value = float(match.group(1))
        raw_unit = match.group(2).strip().lower()
        
        # Try to find the unit in our mapping
        converted_unit = UNIT_MAPPING.get(raw_unit)
        
        # If not found directly, search for any unit from mapping within the raw_unit string
        # (for cases like "vials inhal sol" -> find "vials" and map to 'vial')
        if not converted_unit:
            for known_unit, dmd_unit in UNIT_MAPPING.items():
                if known_unit in raw_unit:
                    converted_unit = dmd_unit
                    break
        
        return value, converted_unit
    
    return None, None


def map_dosage_form_to_form_and_route(df: pd.DataFrame) -> pd.DataFrame:
    """Map dosage form to form and route columns using DOSAGE_FORM_MAPPING.
    
    Args:
        df: DataFrame with a 'dosage_form' column
        
    Returns:
        DataFrame with 'form' and 'route' columns added
    """
    logger = get_run_logger()

    def get_form_and_route(dosage_form):
        if pd.isna(dosage_form) or not dosage_form:
            return None, None
        
        normalised = str(dosage_form).strip().lower()

        if normalised in DOSAGE_FORM_MAPPING:
            mapping = DOSAGE_FORM_MAPPING[normalised]
            return mapping.get('form'), mapping.get('route')

        return None, None
    
    mapped_data = df['dosage_form'].apply(get_form_and_route)
    df['form'] = mapped_data.apply(lambda x: x[0] if x else None)
    df['route'] = mapped_data.apply(lambda x: x[1] if x else None)
    
    unmapped = df[
        df['dosage_form'].notna()
        & (df['dosage_form'].astype(str).str.strip() != '')
        & df['form'].isna()
    ]
    if not unmapped.empty:
        for dosage_form in unmapped['dosage_form'].unique():
            logger.warning(f"Cannot find dosage form mapping for WHO dosage form: {dosage_form!r}")
    
    return df


@task
def process_combined_products(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    
    column_mapping = {
        'ATC Code': 'atc_code',
        'Brand name': 'brand_name',
        'Dosage form': 'dosage_form',
        'Active ingredients per unit dose (UD)': 'active_ingredients_raw',
        'DDD comb.': 'ddd_comb'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Filter out rows that don't have entries in all required columns
    # This excludes products where the DDD is based on dosing frequency - we are ignoring these for now
    initial_count = len(df)
    df = df.dropna(subset=['dosage_form', 'active_ingredients_raw', 'ddd_comb'])
    
    df = df[
        (df['dosage_form'].astype(str).str.strip() != '') &
        (df['active_ingredients_raw'].astype(str).str.strip() != '') &
        (df['ddd_comb'].astype(str).str.strip() != '')
    ]
    filtered_count = len(df)
    if initial_count != filtered_count:
        logger.info(
            f"Filtered out {initial_count - filtered_count} rows with missing required columns "
            f"(excludes products where DDD is based on dosing frequency)"
        )
    
    df = map_dosage_form_to_form_and_route(df)
    
    df['active_ingredients'] = df['active_ingredients_raw'].apply(extract_ingredients_quantity_unit)
    
    df['ddd_ud_value'] = df['ddd_comb'].apply(extract_ud_value)
    
    converted_data = df['ddd_comb'].apply(extract_converted_value_and_unit)
    df['ddd_converted_value'] = converted_data.apply(lambda x: x[0] if x else None)
    df['ddd_converted_unit'] = converted_data.apply(lambda x: x[1] if x else None)
    

    df = df[[
        'atc_code',
        'brand_name',
        'dosage_form',
        'form',
        'route',
        'active_ingredients',
        'ddd_comb',
        'ddd_ud_value',
        'ddd_converted_value',
        'ddd_converted_unit'
    ]]
    
    logger.info(f"Processed {len(df)} combined product records")
    return df


@task
def load_to_bigquery(df: pd.DataFrame):
    logger = get_run_logger()
    table_spec = WHO_DDD_COMBINED_PRODUCTS_TABLE_SPEC
    
    logger.info(
        f"Loading {len(df)} rows to {table_spec.full_table_id}"
    )

    client = get_bigquery_client()

    job_config = bigquery.LoadJobConfig(
        schema=table_spec.schema,
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(
        df, table_spec.full_table_id, job_config=job_config
    )
    job.result()

    logger.info(
        f"Successfully loaded {len(df)} rows to "
        f"{table_spec.full_table_id}"
    )


@flow(name="Import DDD Combined Products")
def import_ddd_combined_products():
    logger = get_run_logger()
    logger.info("Starting DDD combined products import flow")

    try:
        df = fetch_csv_from_gcs()
    
        processed_df = process_combined_products(df)
        
        if not processed_df.empty:
            load_to_bigquery(processed_df)
            logger.info("Completed DDD combined products import flow")
        else:
            logger.warning("No data to load")

    finally:
        cleanup_temp_files(TEMP_DIR)


if __name__ == "__main__":
    import_ddd_combined_products()
