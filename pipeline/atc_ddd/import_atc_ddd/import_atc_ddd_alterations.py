from pathlib import Path
from typing import List, Tuple
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
from pipeline.setup.bq_tables import (
    WHO_DDD_ALTERATIONS_TABLE_SPEC,
    WHO_ATC_ALTERATIONS_TABLE_SPEC,
)

GCS_BASE_PATH = "who_atc_ddd_op_hosp"
FILE_PATTERN = "ATC_DDD_new_and_alterations_"
TEMP_DIR = Path("temp/atc_ddd_alterations")


@task
def find_alterations_files() -> List[Tuple[str, int]]:
    """Find all alterations Excel files in GCS bucket"""
    logger = get_run_logger()
    logger.info(f"Finding alterations files in {BUCKET_NAME}/{GCS_BASE_PATH}")

    bq_client = get_bigquery_client()
    storage_client = storage.Client(
        project=bq_client.project,
        credentials=bq_client._credentials
    )

    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=GCS_BASE_PATH)

    files = []
    for blob in blobs:
        if FILE_PATTERN in blob.name and blob.name.endswith('.xlsx'):

            # format: ATC_DDD_new_and_alterations_YYYY.xlsx
            filename = blob.name.split('/')[-1]
            parts = filename.replace('.xlsx', '').split('_')

            year = None
            for part in parts:
                if part.isdigit() and len(part) == 4:
                    year = int(part)
                    break

            if year:
                files.append((blob.name, year))
                logger.info(
                    f"Found file: {blob.name} (year: {year})"
                )
            else:
                logger.warning(
                    f"Could not extract year from filename: "
                    f"{blob.name}"
                )

    return sorted(files, key=lambda x: x[1])


@task
def fetch_excel_from_gcs(gcs_path: str, year: int) -> tuple[dict, int]:
    """Download Excel file from GCS and read it"""
    logger = get_run_logger()
    logger.info(f"Fetching {gcs_path} from GCS")

    file_path = download_from_gcs(BUCKET_NAME, gcs_path, TEMP_DIR)
    dfs = pd.read_excel(file_path, sheet_name=None)
    return dfs, year


@task
def process_new_atc_5th_levels(dfs: dict, implementation_year: int) -> pd.DataFrame:
    """Process new ATC 5th level codes from the Excel file and format them to match ATC alterations table"""
    logger = get_run_logger()

    new_atc_5th_level_df = dfs.get('New ATC 5th levels', pd.DataFrame())

    if new_atc_5th_level_df.empty:
        logger.warning("No new ATC codes found in Excel file")
        return pd.DataFrame()

    automatic_comment = 'New code'

    if 'Note' in new_atc_5th_level_df.columns:
        alterations_comments = new_atc_5th_level_df['Note'].apply(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip() else None
        )
    else:
        alterations_comments = None


    processed_df = pd.DataFrame({
        'substance': new_atc_5th_level_df['Substance name'],
        'previous_atc_code': None,
        'new_atc_code': new_atc_5th_level_df['New ATC code'],
        'year_changed': implementation_year,
        'comment': alterations_comments,
        'alterations_comment': automatic_comment
    })

    logger.info(f"Processed {len(processed_df)} new ATC codes")
    return processed_df


@task
def process_new_atc_34_levels(dfs: dict, implementation_year: int) -> pd.DataFrame:
    """Process new 3rd and 4th level ATC codes from the Excel file and format them to match ATC alterations table"""
    logger = get_run_logger()

    # Historically the sheet names have not been consistent
    new_atc_34_df = dfs.get("New ATC 3rd and 4th levels", pd.DataFrame())
    if new_atc_34_df.empty:
        new_atc_34_df = dfs.get("New 3rd and 4th levels", pd.DataFrame())

    if new_atc_34_df.empty:
        logger.warning("No new 3rd and 4th level ATC codes found in Excel file")
        return pd.DataFrame()

    automatic_comment = "New 3rd/4th level code"

    if 'Note' in new_atc_34_df.columns:
        alterations_comments = new_atc_34_df['Note'].apply(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip() else None
        )
    else:
        alterations_comments = None

    processed_df = pd.DataFrame(
        {
            "substance": new_atc_34_df["New ATC level name"],
            "previous_atc_code": None,
            "new_atc_code": new_atc_34_df["ATC code"],
            "year_changed": implementation_year,
            "comment": alterations_comments,
            "alterations_comment": automatic_comment,
        }
    )

    logger.info(f"Processed {len(processed_df)} new 3rd and 4th level ATC codes")
    return processed_df

@task
def process_atc_level_alterations(dfs: dict, implementation_year: int) -> pd.DataFrame:
    """Process ATC level alterations from the Excel file and format them to match ATC alterations table"""
    logger = get_run_logger()

    atc_level_df = dfs.get("ATC level alterations", pd.DataFrame())

    if atc_level_df.empty:
        logger.warning("No ATC level alterations found in Excel file")
        return pd.DataFrame()

    automatic_comment = "Level updated: " + atc_level_df["Previous ATC code"] + " → " + atc_level_df["New ATC code"]
    
    if 'Note' in atc_level_df.columns:
        alterations_comments = atc_level_df['Note'].apply(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip() else None
        )
    else:
        alterations_comments = None

    processed_df = pd.DataFrame(
        {
            "substance": atc_level_df["ATC level name"],
            "previous_atc_code": atc_level_df["Previous ATC code"],
            "new_atc_code": atc_level_df["New ATC code"],
            "year_changed": implementation_year,
            "comment": alterations_comments,
            "alterations_comment": automatic_comment,
        }
    )

    logger.info(f"Processed {len(processed_df)} ATC level alterations")
    return processed_df


@task
def process_atc_name_alterations(dfs: dict, implementation_year: int) -> pd.DataFrame:
    """Process ATC level name alterations from the Excel file and format them to match ATC alterations table"""
    logger = get_run_logger()

    atc_name_df = dfs.get("ATC level name alterations", pd.DataFrame())

    if atc_name_df.empty:
        logger.warning("No ATC level name alterations found in Excel file")
        return pd.DataFrame()

    automatic_comment = "Name updated (code unchanged): " + atc_name_df["Previous ATC level name"] + " → " + atc_name_df["New ATC level name"]

    if 'Note' in atc_name_df.columns:
        alterations_comments = atc_name_df['Note'].apply(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip() else None
        )
    else:
        alterations_comments = None

    processed_df = pd.DataFrame(
        {
            "substance": atc_name_df["New ATC level name"],
            "previous_atc_code": atc_name_df["ATC code"],
            "new_atc_code": atc_name_df["ATC code"],
            "year_changed": implementation_year,
            "comment": alterations_comments,
            "alterations_comment": automatic_comment,
        }
    )

    logger.info(f"Processed {len(processed_df)} ATC level name alterations")
    return processed_df


@task
def process_new_ddds(dfs: dict, implementation_year: int) -> pd.DataFrame:
    """Process new DDDs from the Excel file and format them to match DDD alterations table"""
    logger = get_run_logger()
    
    new_ddd_df = dfs.get('New DDDs', pd.DataFrame())
    
    if new_ddd_df.empty:
        logger.warning("No new DDDs found in Excel file")
        return pd.DataFrame()

    automatic_comment = 'New DDD'
    
    if 'Note' in new_ddd_df.columns:
        alterations_comments = new_ddd_df['Note'].apply(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip() else None
        )
    else:
        alterations_comments = None

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
        'comment': alterations_comments,
        'alterations_comment': automatic_comment
    })
    
    logger.info(f"Processed {len(processed_df)} new DDDs")
    return processed_df

@task
def process_ddd_alterations(dfs: dict, implementation_year: int) -> pd.DataFrame:
    """Process DDD alterations from the Excel file and format them to match DDD alterations table"""
    logger = get_run_logger()
    
    # Historically the sheet names are not consistent across years
    ddd_alterations_df = dfs.get('DDD alterations', pd.DataFrame())
    if ddd_alterations_df.empty:
        ddd_alterations_df = dfs.get('DDDs alterations', pd.DataFrame())
    
    if ddd_alterations_df.empty:
        logger.warning("No DDD alterations found in Excel file")
        return pd.DataFrame()

    # there are two "Unit" and "Adm.route" columns - one for the previous DDD and one for the new DDD. 
    # they can be distinguished by their location

    previous_ddd_unit_column_index = (
        ddd_alterations_df.columns.get_loc('Previous DDD') + 1
    )
    ddd_alterations_df.rename(
        columns={
            ddd_alterations_df.columns[previous_ddd_unit_column_index]:
            'Previous DDD unit'
        },
        inplace=True
    )

    new_ddd_unit_column_index = (
        ddd_alterations_df.columns.get_loc('New DDD') + 1
    )
    ddd_alterations_df.rename(
        columns={
            ddd_alterations_df.columns[new_ddd_unit_column_index]:
            'New DDD unit'
        },
        inplace=True
    )

    previous_route_column_index = (
        ddd_alterations_df.columns.get_loc('Previous DDD') + 2
    )
    ddd_alterations_df.rename(
        columns={
            ddd_alterations_df.columns[previous_route_column_index]:
            'Previous route'
        },
        inplace=True
    )

    new_route_column_index = (
        ddd_alterations_df.columns.get_loc('New DDD') + 2
    )
    ddd_alterations_df.rename(
        columns={
            ddd_alterations_df.columns[new_route_column_index]:
            'New route'
        },
        inplace=True
    )

    automatic_comment = 'DDD alteration'

    if 'Note' in ddd_alterations_df.columns:
        alterations_comments = ddd_alterations_df['Note'].apply(
            lambda x: str(x).strip() if pd.notna(x) and str(x).strip() else None
        )
    else:
        alterations_comments = None

    processed_df = pd.DataFrame({
        'substance': ddd_alterations_df['ATC level name'],
        'previous_ddd': ddd_alterations_df['Previous DDD'],
        'previous_ddd_unit': ddd_alterations_df['Previous DDD unit'],
        'previous_route': ddd_alterations_df['Previous route'],
        'new_ddd': ddd_alterations_df['New DDD'],
        'new_ddd_unit': ddd_alterations_df['New DDD unit'],
        'new_route': ddd_alterations_df['New route'],
        'atc_code': ddd_alterations_df['ATC code'],
        'year_changed': implementation_year,
        'comment': alterations_comments,
        'alterations_comment': automatic_comment
    })
    
    logger.info(f"Processed {len(processed_df)} DDD alterations")
    return processed_df


@task
def load_to_bigquery(df: pd.DataFrame, table_spec):
    """Load DataFrame to BigQuery (replaces all existing data)"""
    logger = get_run_logger()
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


@flow(name="Import ATC DDD Alterations")
def import_atc_ddd_alterations():
    """Import ATC and DDD alterations from GCS into BigQuery.
    This processes data from the yearly updates Excel file which includes:
        - New ATC codes at the 5th level
        - New ATC codes at 3rd and 4th levels
        - Changes to ATC level names (where the code remains the same)
        - New DDD assignments for substances
    """
    logger = get_run_logger()
    logger.info("Starting ATC DDD alterations import flow")

    try:
        files = find_alterations_files()

        if not files:
            logger.error("No alterations files found in GCS")
            return

        all_ddd_dfs = []
        all_atc_dfs = []

        for gcs_path, year in files:
            logger.info(
                f"Processing file for year {year}: {gcs_path}"
            )

            excel_dfs, implementation_year = fetch_excel_from_gcs(
                gcs_path, year
            )

            ddd_df = process_new_ddds(excel_dfs, implementation_year)
            if not ddd_df.empty:
                if not ddd_df.empty:
                    all_ddd_dfs.append(ddd_df)
                    logger.info(
                        f"Added {len(ddd_df)} new DDD records from "
                        f"year {year}"
                    )

            ddd_alterations_df = process_ddd_alterations(excel_dfs, implementation_year)
            if not ddd_alterations_df.empty:
                if not ddd_alterations_df.empty:
                    all_ddd_dfs.append(ddd_alterations_df)
                    logger.info(
                        f"Added {len(ddd_alterations_df)} DDD alteration records from "
                        f"year {year}"
                    )

            year_atc_dfs = []

            new_atc_5th_level_df = process_new_atc_5th_levels(
                excel_dfs, implementation_year
            )
            if not new_atc_5th_level_df.empty:
                year_atc_dfs.append(new_atc_5th_level_df)

            new_atc_34_df = process_new_atc_34_levels(
                excel_dfs, implementation_year
            )
            if not new_atc_34_df.empty:
                year_atc_dfs.append(new_atc_34_df)

            atc_name_df = process_atc_name_alterations(
                excel_dfs, implementation_year
            )
            if not atc_name_df.empty:
                year_atc_dfs.append(atc_name_df)

            atc_level_df = process_atc_level_alterations(
                excel_dfs, implementation_year
            )
            if not atc_level_df.empty:
                year_atc_dfs.append(atc_level_df)

            if year_atc_dfs:
                year_combined = pd.concat(
                    year_atc_dfs, ignore_index=True
                )
                all_atc_dfs.append(year_combined)
                logger.info(
                    f"Added {len(year_combined)} ATC records from "
                    f"year {year}"
                )

            
        if all_ddd_dfs:
            combined_ddd_df = pd.concat(all_ddd_dfs, ignore_index=True)
            logger.info(
                f"Total DDD records aggregated: {len(combined_ddd_df)}"
            )
            load_to_bigquery(
                combined_ddd_df, WHO_DDD_ALTERATIONS_TABLE_SPEC
            )
        else:
            logger.warning("No DDD data to load")

        if all_atc_dfs:
            combined_atc_df = pd.concat(all_atc_dfs, ignore_index=True)
            logger.info(
                f"Total ATC records aggregated: {len(combined_atc_df)}"
            )
            load_to_bigquery(
                combined_atc_df, WHO_ATC_ALTERATIONS_TABLE_SPEC
            )
        else:
            logger.warning("No ATC data to load")

        logger.info("Completed ATC DDD alterations import flow")

    finally:
        cleanup_temp_files(TEMP_DIR)


if __name__ == "__main__":
    import_atc_ddd_alterations()
