from pathlib import Path
from typing import Dict, List, Set, Tuple

import pandas as pd
from prefect import task, flow
from prefect.logging import get_run_logger

from pipeline.utils.utils import (
    get_bigquery_client, 
    download_from_gcs, 
    parse_xml, 
    load_to_bigquery, 
    cleanup_temp_files,
    fetch_table_data_from_bq
)
from pipeline.setup.config import PROJECT_ID, DATASET_ID, BUCKET_NAME
from pipeline.setup.bq_tables import WHO_DDD_TABLE_SPEC, WHO_DDD_ALTERATIONS_TABLE_SPEC

DDD_XML_PATH = "who_atc_ddd_op_hosp/2024 ATC_ddd.xml"
TEMP_DIR = Path("temp/ddd")

DDD_COLUMN_MAPPING = {
    "ATCCode": "atc_code",
    "DDD": "ddd",
    "UnitType": "ddd_unit",
    "AdmCode": "adm_code",
    "DDDComment": "comment",
}

@task
def fetch_ddd_alterations() -> pd.DataFrame:
    """Fetch DDD alterations from BigQuery"""
    return fetch_table_data_from_bq(WHO_DDD_ALTERATIONS_TABLE_SPEC)

def split_routes(route_str: str) -> List[str]:
    """
    Split route string into list of individual routes.
    Multiple routes with the same DDD are stored in a single row.
    """
    if pd.isna(route_str):
        return []
    return [r.strip() for r in route_str.split(',') if r.strip()]

@task
def create_ddd_mappings(ddd_alterations: pd.DataFrame) -> Tuple[Dict, List[Dict], Dict, Set]:
    """
    Create mappings for DDD changes and identify new DDDs.
    """
    logger = get_run_logger()
    
    ddd_alterations_sorted = ddd_alterations.sort_values('year_changed')
    
    ddd_updates = {}
    new_ddds = []
    ddds_to_delete = set()
    
    for _, row in ddd_alterations_sorted.iterrows():
        atc_code = row['atc_code']
        new_ddd = row['new_ddd']
        new_unit = row['new_ddd_unit']
        previous_ddd = row['previous_ddd']
        
        new_routes = split_routes(row['new_route'])
        previous_routes = split_routes(row['previous_route'])
            
        comment = row.get('comment')
        if pd.isna(comment) or comment is None:
            comment = None
        else:
            comment = comment.strip() if comment.strip() else None
        
        if (pd.isna(new_ddd) and pd.notna(previous_ddd)):
            # Process DDD deletions
            for prev_route in previous_routes:
                key = (atc_code, prev_route)
                ddds_to_delete.add(key)
            continue
                    
        # Process new DDDs
        if pd.isna(previous_ddd):
            # Use new comment if present, otherwise None
            comment = comment if comment and comment.strip() else None
            
            for route in new_routes:
                new_ddds.append({
                    'atc_code': atc_code,
                    'ddd': float(new_ddd),
                    'ddd_unit': new_unit.lower(),
                    'adm_code': route,
                    'comment': comment
                })
            continue
            
        # Process DDD alterations
        if pd.notna(previous_ddd) and pd.notna(new_ddd):
            for prev_route in previous_routes:
                key = (atc_code, prev_route)
                for new_route in new_routes:
                    ddd_updates[key] = {
                        'new_ddd': float(new_ddd),
                        'new_ddd_unit': new_unit.lower(),
                        'new_route': new_route,
                        'year_changed': row['year_changed'],
                        'comment': comment if comment and comment.strip() else None
                    }
                    
    logger.info(f"Found {len(new_ddds)} new DDD entries to add (after route splitting)")
    logger.info(f"Found {len(ddd_updates)} DDD alterations to process (after route splitting)")
    logger.info(f"Found {len(ddds_to_delete)} DDD entries to delete (after route splitting)")

    return ddd_updates, new_ddds, ddds_to_delete


def apply_ddd_deletions_and_updates(ddd_df: pd.DataFrame, ddd_updates: Dict, ddds_to_delete: Set) -> pd.DataFrame:
    """Apply DDD deletions and updates"""
    logger = get_run_logger()
    logger.info(f"Processing {len(ddd_updates)} DDD alterations and {len(ddds_to_delete)} deletions")
    
    updated_df = ddd_df.copy()
    updates_made = 0
    deletions_made = 0
    new_rows = []
    
    def get_comment(existing_comment, new_comment):
        """Get comment: replace with new comment if present, otherwise keep existing"""
        if new_comment and new_comment.strip():
            return new_comment.strip()
        # If no new comment, keep existing comment
        if pd.notna(existing_comment) and existing_comment.strip():
            return existing_comment.strip()
        return None
    
    for atc_code, route in ddds_to_delete:
        mask = (
            (updated_df['atc_code'] == atc_code) & 
            (updated_df['adm_code'] == route)
        )
        if mask.any():
            updated_df = updated_df[~mask]
            deletions_made += mask.sum()

    for (atc_code, prev_route), update_info in ddd_updates.items():
        mask = (
            (updated_df['atc_code'] == atc_code) & 
            (updated_df['adm_code'] == prev_route)
        )
        
        if mask.any():
            existing_comment = updated_df[mask].iloc[0].get('comment', '')
            new_comment = get_comment(
                existing_comment,
                update_info.get('comment')
            )
            
            # If the route is changing, add a new row instead of updating in place
            if update_info['new_route'] != prev_route:
                row_data = updated_df[mask].iloc[0].to_dict()
                row_data.update({
                    'ddd': update_info['new_ddd'],
                    'ddd_unit': update_info['new_ddd_unit'],
                    'adm_code': update_info['new_route'],
                    'comment': new_comment
                })
                new_rows.append(row_data)
                # Remove the old route entry
                updated_df = updated_df[~mask]
            else:
                updated_df.loc[mask, 'ddd'] = update_info['new_ddd']
                updated_df.loc[mask, 'ddd_unit'] = update_info['new_ddd_unit']
                updated_df.loc[mask, 'comment'] = new_comment
            updates_made += 1
    
    if new_rows:
        updated_df = pd.concat([updated_df, pd.DataFrame(new_rows)], ignore_index=True)
    
    logger.info(f"Deleted {deletions_made} DDD entries")
    logger.info(f"Updated {updates_made} DDD entries")
    logger.info(f"Created {len(new_rows)} new route entries from updates")
    
    return updated_df

@task
def process_ddd_data(
    ddd_df: pd.DataFrame, 
    ddd_updates: Dict, 
    ddds_to_delete: Set, 
    new_ddds: List[Dict]
) -> pd.DataFrame:
    """
    Process DDD data by applying updates, deletions, and additions.
    """
    logger = get_run_logger()
    logger.info(f"Processing {len(ddd_df)} DDDs")
    
    ddd_df = ddd_df.copy()

    ddd_df.loc[:, 'comment'] = ddd_df['comment'].apply(lambda x: None if pd.isna(x) or not str(x).strip() else str(x).strip())
    ddd_df.loc[:, "adm_code"] = ddd_df["adm_code"].str.strip()
    ddd_df.loc[:, "ddd_unit"] = ddd_df["ddd_unit"].str.lower()
    ddd_df.loc[:, "ddd"] = ddd_df["ddd"].apply(lambda x: float(x) if x else None)
    ddd_df = ddd_df[ddd_df["ddd"].notna()]
    
    ddd_df = apply_ddd_deletions_and_updates(ddd_df, ddd_updates, ddds_to_delete)

    if new_ddds:
        new_ddd_df = pd.DataFrame(new_ddds)
        ddd_df = pd.concat([ddd_df, new_ddd_df], ignore_index=True)
        logger.info(f"{len(new_ddd_df)} new DDDs added")
    
    return ddd_df


@flow(name="Import DDD Data")
def import_ddd() -> Dict[str, int]:
    """
    Main flow to import and update DDD data with alterations.
    
    Returns:
        Dictionary containing processing statistics
    """
    logger = get_run_logger()
    logger.info("Starting DDD import flow")

    try:
        ddd_alterations = fetch_ddd_alterations()

        ddd_updates, new_ddds, ddds_to_delete = create_ddd_mappings(ddd_alterations)

        ddd_path = download_from_gcs(BUCKET_NAME, DDD_XML_PATH, TEMP_DIR)
        ddd_df = parse_xml(ddd_path)

        ddd_df = ddd_df.rename(columns=DDD_COLUMN_MAPPING)

        updated_ddd_df = process_ddd_data(ddd_df, ddd_updates, ddds_to_delete, new_ddds)

        # validate_who_routes(updated_ddd_df)
        load_to_bigquery(updated_ddd_df, WHO_DDD_TABLE_SPEC)

        logger.info("DDD import completed successfully")


    finally:
        cleanup_temp_files(TEMP_DIR)


if __name__ == "__main__":
    import_ddd()
