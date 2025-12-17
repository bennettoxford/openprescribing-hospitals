import re
from pathlib import Path
from typing import Optional, Dict, Tuple

import pandas as pd
from prefect import task, flow
from prefect.logging import get_run_logger

from pipeline.utils.utils import (
    download_from_gcs, 
    parse_xml, 
    load_to_bigquery, 
    cleanup_temp_files,
    fetch_table_data_from_bq
)
from pipeline.setup.config import BUCKET_NAME
from pipeline.setup.bq_tables import WHO_ATC_TABLE_SPEC, WHO_ATC_ALTERATIONS_TABLE_SPEC

ATC_XML_PATH = "who_atc_ddd_op_hosp/2024 ATC.xml"
TEMP_DIR = Path("temp/atc")

ATC_COLUMN_MAPPING = {
    "ATCCode": "atc_code",
    "Name": "atc_name", 
    "Comment": "comment",
}

class ATCLevel:
    """ATC classification levels"""
    ANATOMICAL_MAIN_GROUP = 1
    THERAPEUTIC_SUBGROUP = 2
    PHARMACOLOGICAL_SUBGROUP = 3
    CHEMICAL_SUBGROUP = 4
    CHEMICAL_SUBSTANCE = 5


def get_atc_level(code: str) -> Optional[int]:
    """
    Get the ATC classification level based on the code length.
    
    Args:
        code: The ATC code
        
    Returns:
        The ATC level (1-5) or None if invalid
    """
    if not isinstance(code, str):
        return None
        
    code = code.strip()
    
    if not re.match(r'^[A-Z](?:[0-9]{2})?[A-Z]?[A-Z]?(?:[0-9]{2})?$', code):
        return None
        
    length_to_level = {
        1: ATCLevel.ANATOMICAL_MAIN_GROUP,
        3: ATCLevel.THERAPEUTIC_SUBGROUP,
        4: ATCLevel.PHARMACOLOGICAL_SUBGROUP,
        5: ATCLevel.CHEMICAL_SUBGROUP,
        7: ATCLevel.CHEMICAL_SUBSTANCE,
    }
    
    return length_to_level.get(len(code))


def convert_atc_name(name: str) -> Optional[str]:
    """
    Convert ATC names to proper case format.
    
    Args:
        name: The ATC name to convert
        
    Returns:
        The converted name or None if input is None
    """
    if name is None:
        return None

    name = name.strip()
    if not name:
        return ""
        
    # Handle all uppercase words
    if name.isupper():
        return name.title()
    
    # Handle mixed case by converting to title case
    return ' '.join(word.capitalize() for word in name.split())

@task
def fetch_atc_alterations() -> pd.DataFrame:
    """Fetch ATC alterations from BigQuery"""
    return fetch_table_data_from_bq(WHO_ATC_ALTERATIONS_TABLE_SPEC)


@task
def create_atc_code_mapping(atc_alterations: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    """
    Create mappings for ATC code changes and new codes.
    
    Returns:
        Tuple of:
            - Mapping of old ATC codes to their current codes
            - Mapping of brand new ATC codes (no previous) to their substances  
            - Mapping of replacement ATC codes (has previous) to their substances
    """
    logger = get_run_logger()

    if len(atc_alterations) == 0:
        logger.info("No ATC alterations to process")
        return {}, {}, {}
    
    atc_alterations_sorted = atc_alterations.sort_values('year_changed')
    
    code_mapping = {}
    new_codes = {}
    deleted_codes = {}
    
    for _, row in atc_alterations_sorted.iterrows():
        old_code = row['previous_atc_code']
        new_code = row['new_atc_code']
        substance = row['substance']

        comment = row.get('comment')
        if pd.isna(comment) or comment is None:
            comment = None
        else:
            comment = comment.strip() if comment.strip() else None
        
        # Handle deleted codes
        if new_code == 'deleted':
            deleted_codes[old_code] = substance
            continue
        
        # Handle new codes
        if pd.isna(old_code):
            new_codes[new_code] = {
                'substance': substance,
                'comment': comment
            }
            continue

        else:
            code_mapping[old_code] = {
                'new_code': new_code,
                'substance': substance,
                'comment': comment
            }
    
    logger.info(f"Created mapping for {len(code_mapping)} ATC codes")
    logger.info(f"Found {len(new_codes)} brand new ATC codes")
    logger.info(f"Found {len(deleted_codes)} deleted ATC codes")

    return code_mapping, new_codes, deleted_codes

@task
def process_atc_data(
    atc_df: pd.DataFrame, 
    atc_mapping: Dict[str, Dict[str, str]], 
    new_codes: Dict[str, Dict[str, str]],
    deleted_codes: Dict[str, str]
) -> pd.DataFrame:
    """
    Process ATC data by adding new codes, updating existing codes, and enriching with hierarchical data.
    """
    logger = get_run_logger()


    atc_df = atc_df.copy()

    atc_df.loc[:, 'comment'] = atc_df['comment'].apply(lambda x: None if pd.isna(x) or not str(x).strip() else str(x).strip())
    
    def get_comment(existing_comment, new_comment):
        """Get comment: replace with new comment if present, otherwise keep existing"""
        if new_comment and new_comment.strip():
            return new_comment.strip()
        # If no new comment, keep existing comment
        if pd.notna(existing_comment) and existing_comment.strip():
            return existing_comment.strip()
        return None
    
    # Delete codes
    deletions_made = 0
    for code, substance in deleted_codes.items():
        mask = atc_df['atc_code'] == code
        if mask.any():
            atc_df = atc_df[~mask].copy()
            deletions_made += mask.sum()
    
    if deletions_made > 0:
        logger.info(f"Deleted {deletions_made} ATC codes")
    else:
        logger.info("No ATC codes were deleted")

    # Add new ATC codes
    new_rows = []
    for code, code_info in new_codes.items():
        substance = code_info['substance']
        comment = code_info.get('comment')
        
        new_rows.append({
            'atc_code': code,
            'atc_name': substance,
            'comment': comment
        })
    
    if new_rows:
        logger.info(f"Adding {len(new_rows)} new ATC codes")
        new_codes_df = pd.DataFrame(new_rows)
        atc_df = pd.concat([atc_df, new_codes_df], ignore_index=True)
    
    # Update existing codes
    if atc_mapping:
        logger.info(f"Updating {len(atc_mapping)} ATC code mappings")
        
        for old_code, update_info in atc_mapping.items():
            mask = atc_df['atc_code'] == old_code
            if mask.any():
                existing_comment = atc_df.loc[mask, 'comment'].iloc[0]
                new_comment = get_comment(
                    existing_comment,
                    update_info.get('comment')
                )

                atc_df.loc[mask, 'atc_code'] = update_info['new_code']
                atc_df.loc[mask, 'atc_name'] = update_info['substance']
                atc_df.loc[mask, 'comment'] = new_comment
    
    atc_df['atc_name'] = atc_df['atc_name'].apply(convert_atc_name)
    
    add_atc_hierarchy(atc_df)
    
    return atc_df

def add_atc_hierarchy(atc_df: pd.DataFrame) -> None:
    """Add hierarchical ATC information to the DataFrame"""
    code_to_name_mapping = atc_df.set_index('atc_code')['atc_name'].to_dict()
    
    atc_df['level'] = atc_df['atc_code'].apply(get_atc_level)

    atc_df['anatomical_main_group'] = None
    atc_df['therapeutic_subgroup'] = None
    atc_df['pharmacological_subgroup'] = None
    atc_df['chemical_subgroup'] = None
    atc_df['chemical_substance'] = None

    for idx, row in atc_df.iterrows():
        level = row['level']
        code = row['atc_code']
        
        if level is None:
            continue
            
        # Level 1+: Anatomical main group
        if level >= 1:
            atc_df.at[idx, 'anatomical_main_group'] = code_to_name_mapping.get(code[:1])
        
        # Level 2+: Therapeutic subgroup
        if level >= 2:
            atc_df.at[idx, 'therapeutic_subgroup'] = code_to_name_mapping.get(code[:3])
        
        # Level 3+: Pharmacological subgroup
        if level >= 3:
            atc_df.at[idx, 'pharmacological_subgroup'] = code_to_name_mapping.get(code[:4])
        
        # Level 4+: Chemical subgroup
        if level >= 4:
            atc_df.at[idx, 'chemical_subgroup'] = code_to_name_mapping.get(code[:5])
        
        # Level 5: Chemical substance
        if level >= 5:
            atc_df.at[idx, 'chemical_substance'] = code_to_name_mapping.get(code[:7])


@flow(name="Import ATC Data")
def import_atc() -> Dict[str, int]:
    """
    Main flow to import and update ATC data with alterations.
    
    Returns:
        Dictionary containing processing statistics
    """
    logger = get_run_logger()
    logger.info("Starting ATC import flow")

    try:
        atc_alterations = fetch_atc_alterations()
        
        atc_mapping, new_atc_codes, deleted_codes = create_atc_code_mapping(atc_alterations)

        atc_path = download_from_gcs(BUCKET_NAME, ATC_XML_PATH, TEMP_DIR)
        atc_df = parse_xml(atc_path)

        atc_df = atc_df.rename(columns=ATC_COLUMN_MAPPING)

        updated_atc_df = process_atc_data(
            atc_df, atc_mapping, new_atc_codes, deleted_codes
        )

        load_to_bigquery(updated_atc_df, WHO_ATC_TABLE_SPEC)

        logger.info("ATC import completed successfully")
        

    finally:
        cleanup_temp_files(TEMP_DIR)


if __name__ == "__main__":
    import_atc()
