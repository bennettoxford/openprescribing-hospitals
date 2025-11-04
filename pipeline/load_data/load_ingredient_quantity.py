import pandas as pd
import time
import argparse

from google.cloud import bigquery
from prefect import get_run_logger, task, flow
from django.db import transaction
from typing import Dict, List, Tuple
from pipeline.setup.bq_tables import INGREDIENT_QUANTITY_TABLE_SPEC, CALCULATION_LOGIC_TABLE_SPEC
from pipeline.utils.utils import (
    setup_django_environment,
    get_bigquery_client,
    execute_bigquery_query,
)


setup_django_environment()

from viewer.models import IngredientQuantity, Ingredient, VMP, Organisation, CalculationLogic


@task
def get_ingredient_calculation_logic() -> Dict[Tuple[str, str], str]:
    """Download calculation logic for ingredient calculations from the calculation logic table"""
    logger = get_run_logger()
    
    client = get_bigquery_client()

    query = f"""
    SELECT 
        vmp_code,
        ingredient_code,
        logic
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type = 'ingredient'
    """

    result = client.query(query).to_dataframe(create_bqstorage_client=True)
    
    logic_dict = {}
    for _, row in result.iterrows():
        ingredient_code = row['ingredient_code'] if pd.notna(row['ingredient_code']) else None
        key = (row['vmp_code'], ingredient_code)
        logic_dict[key] = row['logic']
    
    logger.info(f"Downloaded ingredient calculation logic for {len(logic_dict):,} VMP-ingredient combinations")
    return logic_dict


@task
def get_all_vmp_ingredient_combinations_with_logic() -> List[Tuple[str, str]]:
    """Get all unique VMP-ingredient combinations that have calculation logic"""
    logger = get_run_logger()
    
    client = get_bigquery_client()

    query = f"""
    SELECT DISTINCT vmp_code, ingredient_code
    FROM `{CALCULATION_LOGIC_TABLE_SPEC.full_table_id}`
    WHERE logic_type = 'ingredient'
    ORDER BY vmp_code, ingredient_code
    """

    result = client.query(query).to_dataframe()
    combinations = []
    for _, row in result.iterrows():
        ingredient_code = row['ingredient_code'] if pd.notna(row['ingredient_code']) else None
        combinations.append((row['vmp_code'], ingredient_code))
    
    logger.info(f"Found {len(combinations):,} unique VMP-ingredient combinations with calculation logic")
    return combinations


@task
def get_unique_vmps_with_ingredient_data() -> List[str]:
    """Get all unique VMP codes that have ingredient quantity data"""
    logger = get_run_logger()

    query = f"""
    SELECT DISTINCT vmp_code
    FROM `{INGREDIENT_QUANTITY_TABLE_SPEC.full_table_id}`
    WHERE EXISTS (
      SELECT 1 
      FROM UNNEST(ingredients) as ing 
      WHERE ing.ingredient_quantity IS NOT NULL
    )
    ORDER BY vmp_code
    """

    results = execute_bigquery_query(query)
    
    vmp_codes = [row["vmp_code"] for row in results]
    logger.info(f"Found {len(vmp_codes):,} unique VMPs with ingredient quantity data")
    return vmp_codes


def fetch_bigquery_data(query: str, client) -> pd.DataFrame:
    """Fetch BigQuery data with automatic memory cleanup"""
    job_config = bigquery.QueryJobConfig(use_query_cache=False, allow_large_results=True)
    query_job = client.query(query, job_config=job_config)
    df = query_job.to_dataframe(create_bqstorage_client=True)
    return df.copy()


@task
def extract_ingredient_data_by_vmps(
    vmp_codes: List[str], chunk_num: int, total_chunks: int
) -> pd.DataFrame:
    """Extract ingredient quantity data for a specific set of VMP codes"""
    logger = get_run_logger()

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Extracting data for {len(vmp_codes):,} VMPs"
    )

    if not vmp_codes:
        logger.info(f"Chunk {chunk_num}/{total_chunks}: No VMPs to extract data for")
        return pd.DataFrame()

    client = get_bigquery_client()

    vmp_list_str = "', '".join(vmp_codes)

    query = f"""
    SELECT 
        vmp_code,
        year_month,
        ods_code,
        ingredients
    FROM `{INGREDIENT_QUANTITY_TABLE_SPEC.full_table_id}`
    WHERE EXISTS (
      SELECT 1 
      FROM UNNEST(ingredients) as ing 
      WHERE ing.ingredient_quantity IS NOT NULL
    )
    AND vmp_code IN ('{vmp_list_str}')
    ORDER BY vmp_code, ods_code, year_month
    """

    df = fetch_bigquery_data(query, client)

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Extracted {len(df):,} rows for {len(vmp_codes):,} VMPs"
    )

    return df


@task
def clear_existing_ingredient_data() -> Tuple[int, int]:
    """Clear all existing ingredient quantity data and calculation logic in chunks"""
    logger = get_run_logger()
    logger.info("Clearing existing ingredient quantity data and calculation logic")

    total_deleted = 0
    total_logic_deleted = 0
    chunk_size = 10_000

    while True:
        with transaction.atomic():
            batch_ids = list(
                CalculationLogic.objects.filter(logic_type='ingredient').values_list("id", flat=True)[:chunk_size]
            )
            if not batch_ids:
                break

            deleted_count = CalculationLogic.objects.filter(
                id__in=batch_ids
            ).delete()[0]
            total_logic_deleted += deleted_count

    while True:
        with transaction.atomic():
            batch_ids = list(
                IngredientQuantity.objects.values_list("id", flat=True)[:chunk_size]
            )
            if not batch_ids:
                break

            deleted_count = IngredientQuantity.objects.filter(
                id__in=batch_ids
            ).delete()[0]
            total_deleted += deleted_count

    logger.info(f"Deleted {total_deleted:,} existing ingredient quantity records")
    logger.info(f"Deleted {total_logic_deleted:,} existing ingredient calculation logic records")
    return total_deleted, total_logic_deleted


@task
def cache_foreign_keys() -> Dict:
    """Cache all Ingredient, VMP and Organisation foreign keys"""
    logger = get_run_logger()
    logger.info("Caching foreign key mappings")

    ingredients = {ing.code: ing.id for ing in Ingredient.objects.all()}
    vmps = {vmp.code: vmp.id for vmp in VMP.objects.all()}
    organisations = {org.ods_code: org.id for org in Organisation.objects.all()}

    logger.info(
        f"Cached {len(ingredients):,} ingredients, {len(vmps):,} VMPs and {len(organisations):,} organisations"
    )
    return {"ingredients": ingredients, "vmps": vmps, "organisations": organisations}


@task
def load_ingredient_logic_for_combinations(
    combinations: List[Tuple[str, str]], 
    foreign_key_cache: Dict, 
    ingredient_logic_dict: Dict[Tuple[str, str], str],
    chunk_num: int, 
    total_chunks: int
) -> Dict:
    """Load ingredient logic for a specific set of VMP-ingredient combinations"""
    logger = get_run_logger()
    
    if len(combinations) == 0:
        return {"logic_created": 0, "logic_missing": 0}
    
    ingredients = foreign_key_cache["ingredients"]
    vmps = foreign_key_cache["vmps"]
    logic_objects = []
    logic_missing = 0
    
    for (vmp_code, ingredient_code) in combinations:
        logic_key = (vmp_code, ingredient_code)
        
        if logic_key in ingredient_logic_dict and vmp_code in vmps:
            if ingredient_code is None:
                logic_objects.append(
                    CalculationLogic(
                        vmp_id=vmps[vmp_code],
                        ingredient_id=None,
                        logic_type='ingredient',
                        logic=ingredient_logic_dict[logic_key]
                    )
                )
            elif ingredient_code in ingredients:
                logic_objects.append(
                    CalculationLogic(
                        vmp_id=vmps[vmp_code],
                        ingredient_id=ingredients[ingredient_code],
                        logic_type='ingredient',
                        logic=ingredient_logic_dict[logic_key]
                    )
                )
            else:
                logger.warning(f"Chunk {chunk_num}/{total_chunks}: Ingredient {ingredient_code} not found in database")
                logic_missing += 1
        elif logic_key not in ingredient_logic_dict:
            logger.warning(f"Chunk {chunk_num}/{total_chunks}: No ingredient logic found for VMP {vmp_code} + Ingredient {ingredient_code}")
            logic_missing += 1
        elif vmp_code not in vmps:
            logger.warning(f"Chunk {chunk_num}/{total_chunks}: VMP {vmp_code} not found in database")
            logic_missing += 1
    
    logic_created = 0
    if logic_objects:
        try:
            with transaction.atomic():
                CalculationLogic.objects.bulk_create(logic_objects)
                logic_created = len(logic_objects)
        except Exception as e:
            logger.error(f"Chunk {chunk_num}/{total_chunks}: Error creating ingredient logic: {str(e)}")
    
    logger.info(f"Chunk {chunk_num}/{total_chunks}: Created {logic_created} ingredient logic records, {logic_missing} missing")
    
    return {"logic_created": logic_created, "logic_missing": logic_missing}


@task
def transform_and_load_ingredient_quantity_chunk(
    chunk_df: pd.DataFrame, 
    foreign_key_cache: Dict, 
    chunk_num: int, 
    total_chunks: int
) -> Dict:
    """Transform and load a chunk of ingredient quantity data"""
    logger = get_run_logger()

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Transforming and loading {len(chunk_df):,} ingredient quantity records"
    )

    if len(chunk_df) == 0:
        logger.info(f"Chunk {chunk_num}/{total_chunks}: No ingredient quantity data to process")
        return {"created": 0, "skipped": 0}

    logger.info(f"Chunk {chunk_num}/{total_chunks}: Filtering for valid records...")
    valid_mask = (
        chunk_df["vmp_code"].notna()
        & chunk_df["ods_code"].notna()
        & chunk_df["year_month"].notna()
        & chunk_df["ingredients"].notna()
    )

    df_valid = chunk_df[valid_mask].copy()
    skipped_count = len(chunk_df) - len(df_valid)

    if skipped_count > 0:
        logger.info(
            f"Chunk {chunk_num}/{total_chunks}: Filtered out {skipped_count:,} records with missing data"
        )

    if len(df_valid) == 0:
        logger.info(
            f"Chunk {chunk_num}/{total_chunks}: No valid ingredient quantity records after filtering"
        )
        return {"created": 0, "skipped": skipped_count}

    df_valid["year_month"] = pd.to_datetime(df_valid["year_month"].astype(str)).dt.strftime(
        "%Y-%m-%d"
    )

    df_valid = df_valid.copy()
    df_valid['ingredients'] = df_valid['ingredients'].apply(
        lambda x: x.tolist() if hasattr(x, 'tolist') else (x if isinstance(x, list) else [])
    )

    ingredient_data = {}

    logger.info(f"Chunk {chunk_num}/{total_chunks}: Processing ingredient data...")

    for row in df_valid.itertuples(index=False):
        try:
            vmp_code = row.vmp_code
            ods_code = row.ods_code
            year_month = row.year_month
            ingredients = row.ingredients

            for ingredient in ingredients:
                ingredient_code = ingredient.get("ingredient_code")
                ingredient_quantity_basis = ingredient.get("ingredient_quantity_basis")
                ingredient_basis_unit = ingredient.get("ingredient_basis_unit")

                if (
                    not ingredient_code
                    or ingredient_quantity_basis is None
                    or not ingredient_basis_unit
                ):
                    continue

                key = (ingredient_code, vmp_code, ods_code)

                if key not in ingredient_data:
                    ingredient_data[key] = []

                ingredient_data[key].append(
                    [
                        year_month,
                        str(float(ingredient_quantity_basis)),
                        ingredient_basis_unit,
                    ]
                )
        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            continue

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Created {len(ingredient_data):,} ingredient-VMP-organisation combinations"
    )

    ingredients = foreign_key_cache["ingredients"]
    vmps = foreign_key_cache["vmps"]
    organisations = foreign_key_cache["organisations"]

    iq_objects = []
    skipped_due_to_missing_fk = 0

    for (ingredient_code, vmp_code, ods_code), data_array in ingredient_data.items():
        if (
            ingredient_code in ingredients
            and vmp_code in vmps
            and ods_code in organisations
        ):
            iq_objects.append(
                IngredientQuantity(
                    ingredient_id=ingredients[ingredient_code],
                    vmp_id=vmps[vmp_code],
                    organisation_id=organisations[ods_code],
                    data=data_array,
                )
            )
        else:
            skipped_due_to_missing_fk += 1

    logger.info(
        f"Chunk {chunk_num}/{total_chunks}: Loading {len(iq_objects):,} objects to database..."
    )

    SUB_BATCH_SIZE = 500
    total_created = 0
    total_skipped = skipped_count + skipped_due_to_missing_fk

    for i in range(0, len(iq_objects), SUB_BATCH_SIZE):
        sub_batch = iq_objects[i : i + SUB_BATCH_SIZE]
        
        try:
            with transaction.atomic():
                IngredientQuantity.objects.bulk_create(
                    sub_batch,
                    batch_size=SUB_BATCH_SIZE,
                    unique_fields=["ingredient", "vmp", "organisation"],
                )
                total_created += len(sub_batch)
        except Exception as e:
            logger.error(
                f"Chunk {chunk_num}/{total_chunks}: Error in sub-batch {i//SUB_BATCH_SIZE + 1}: {str(e)}"
            )
            total_skipped += len(sub_batch)

    return {
        "created": total_created,
        "skipped": total_skipped,
    }


@flow
def load_ingredient_quantity(combination_chunk_size: int = 1000, vmp_chunk_size: int = 500):
    """
    Main flow to import ingredient quantity data from BigQuery

    Args:
        combination_chunk_size: Number of VMP-ingredient combinations to process in each logic chunk (default: 1000)
        vmp_chunk_size: Number of VMPs to process in each quantity chunk (default: 500)
    """
    logger = get_run_logger()
    start_time = time.time()

    logger.info(f"Starting ingredient quantity data import from BigQuery")

    ingredient_logic_dict = get_ingredient_calculation_logic()

    all_logic_combinations = get_all_vmp_ingredient_combinations_with_logic()

    quantity_vmps = get_unique_vmps_with_ingredient_data()

    logger.info(
        f"Found {len(all_logic_combinations):,} VMP-ingredient combinations with logic and {len(quantity_vmps):,} VMPs with ingredient quantity data"
    )

    deleted_count, logic_deleted_count = clear_existing_ingredient_data()
    foreign_key_cache = cache_foreign_keys()

    total_stats = {
        "total_created": 0,
        "total_skipped": 0,
        "total_logic_created": 0,
        "total_logic_missing": 0,
        "total_processed_chunks": 0,
    }

    logic_total_chunks = (len(all_logic_combinations) + combination_chunk_size - 1) // combination_chunk_size
    logger.info(
        f"Will process ingredient logic for {len(all_logic_combinations):,} VMP-ingredient combinations in {logic_total_chunks} chunks of {combination_chunk_size} combinations each"
    )

    for chunk_num in range(1, logic_total_chunks + 1):
        chunk_start_time = time.time()

        start_idx = (chunk_num - 1) * combination_chunk_size
        end_idx = min(start_idx + combination_chunk_size, len(all_logic_combinations))
        chunk_combinations = all_logic_combinations[start_idx:end_idx]

        logic_result = load_ingredient_logic_for_combinations(
            chunk_combinations, foreign_key_cache, ingredient_logic_dict, chunk_num, logic_total_chunks
        )

        total_stats["total_logic_created"] += logic_result["logic_created"]
        total_stats["total_logic_missing"] += logic_result["logic_missing"]

        chunk_duration = time.time() - chunk_start_time
        progress_pct = (chunk_num / logic_total_chunks) * 100

        logger.info(
            f"Logic chunk {chunk_num}/{logic_total_chunks} complete in {chunk_duration:.1f}s ({progress_pct:.1f}% done). "
            f"Processed combinations {start_idx+1}-{end_idx}. "
            f"Running totals - Logic created: {total_stats['total_logic_created']:,}, "
            f"Logic missing: {total_stats['total_logic_missing']:,}. "
        )

    quantity_total_chunks = (len(quantity_vmps) + vmp_chunk_size - 1) // vmp_chunk_size
    logger.info(
        f"Will process ingredient quantity data for {len(quantity_vmps):,} VMPs in {quantity_total_chunks} chunks of {vmp_chunk_size} VMPs each"
    )

    for chunk_num in range(1, quantity_total_chunks + 1):
        chunk_start_time = time.time()

        start_idx = (chunk_num - 1) * vmp_chunk_size
        end_idx = min(start_idx + vmp_chunk_size, len(quantity_vmps))
        chunk_vmps = quantity_vmps[start_idx:end_idx]

        chunk_df = extract_ingredient_data_by_vmps(chunk_vmps, chunk_num, quantity_total_chunks)

        if len(chunk_df) == 0:
            logger.info(
                f"Quantity chunk {chunk_num}: No data returned for VMPs {start_idx+1}-{end_idx}"
            )
            continue

        chunk_result = transform_and_load_ingredient_quantity_chunk(
            chunk_df, foreign_key_cache, chunk_num, quantity_total_chunks
        )

        total_stats["total_created"] += chunk_result["created"]
        total_stats["total_skipped"] += chunk_result["skipped"]
        total_stats["total_processed_chunks"] += 1

        chunk_duration = time.time() - chunk_start_time
        progress_pct = (chunk_num / quantity_total_chunks) * 100

        logger.info(
            f"Quantity chunk {chunk_num}/{quantity_total_chunks} complete in {chunk_duration:.1f}s ({progress_pct:.1f}% done). "
            f"Processed VMPs {start_idx+1}-{end_idx}. "
            f"Running totals - Quantity created: {total_stats['total_created']:,}, "
            f"Quantity skipped: {total_stats['total_skipped']:,}. "
        )

    total_time = time.time() - start_time

    logger.info(
        f"Ingredient quantity data import completed in {total_time/60:.1f} minutes. "
        f"Deleted: {deleted_count:,}, Created: {total_stats['total_created']:,}, "
        f"Logic deleted: {logic_deleted_count:,}, Logic created: {total_stats['total_logic_created']:,}, "
        f"Logic missing: {total_stats['total_logic_missing']:,}, "
        f"Skipped: {total_stats['total_skipped']:,}, Chunks processed: {total_stats['total_processed_chunks']}. "
    )




if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load ingredient quantity data from BigQuery"
    )
    parser.add_argument(
        "--combination-chunk-size",
        type=int,
        default=1000,
        help="Number of VMP-ingredient combinations per logic chunk (default: 1000)",
    )
    parser.add_argument(
        "--vmp-chunk-size",
        type=int,
        default=500,
        help="Number of VMPs per quantity chunk (default: 500)",
    )

    args = parser.parse_args()

    load_ingredient_quantity(
        combination_chunk_size=args.combination_chunk_size,
        vmp_chunk_size=args.vmp_chunk_size
    )
