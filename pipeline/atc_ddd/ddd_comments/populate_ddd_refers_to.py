from prefect import flow, get_run_logger
from pipeline.utils.utils import execute_bigquery_query_from_sql_file, validate_table_schema, get_bigquery_client
from pipeline.setup.bq_tables import DDD_REFERS_TO_TABLE_SPEC
from pathlib import Path
from pipeline.setup.config import PROJECT_ID, DATASET_ID, WHO_DDD_TABLE_ID, DMD_FULL_TABLE_ID


def validate_ddd_refers_to_matches():
    """Validate that all 'refers to' comments in WHO DDD data can be matched to dm+d ingredients"""
    logger = get_run_logger()
    logger.info("Validating DDD 'refers to' matches")

    try:
        client = get_bigquery_client()

        validation_query = f"""
        SELECT
          dc.ddd_comment,
          dc.refers_to_ingredient,
          'No matching dm+d ingredient found' AS validation_error
        FROM (
          -- Extract "refers to" comments from WHO DDD data
          SELECT DISTINCT
            comment AS ddd_comment,
            -- Extract ingredient name from "Refers to [ingredient]" pattern
            CASE
              WHEN LOWER(comment) LIKE 'refers to %' THEN
                TRIM(REGEXP_REPLACE(comment, r'(?i)^refers to[[:space:]]+', ''))
              ELSE NULL
            END AS refers_to_ingredient
          FROM `{PROJECT_ID}.{DATASET_ID}.{WHO_DDD_TABLE_ID}`
          WHERE comment IS NOT NULL
            AND LOWER(comment) LIKE 'refers to %'
            AND LOWER(comment) != 'refers to sc injection' -- This is a different type of DDD comment handled elsewhere
        ) dc
        LEFT JOIN (
          -- Get all unique ingredients from dm+d data
          SELECT DISTINCT
            ingredient.ing_code AS dmd_ingredient_code,
            ingredient.ing_name AS dmd_ingredient_name
          FROM `{PROJECT_ID}.{DATASET_ID}.{DMD_FULL_TABLE_ID}`,
          UNNEST(ingredients) AS ingredient
          WHERE ingredient.ing_code IS NOT NULL
            AND ingredient.ing_name IS NOT NULL
        ) di
        ON (
          STARTS_WITH(LOWER(TRIM(di.dmd_ingredient_name)), LOWER(TRIM(dc.refers_to_ingredient)))
          OR (
            LOWER(TRIM(dc.refers_to_ingredient)) = 'alendronic acid'
            AND LOWER(di.dmd_ingredient_name) LIKE '%alendronate%'
          )
          OR (
            LOWER(TRIM(dc.refers_to_ingredient)) = 'risedronic acid'
            AND LOWER(di.dmd_ingredient_name) LIKE '%risedronate%'
          )
        )
        WHERE dc.refers_to_ingredient IS NOT NULL
          -- Only include cases where no match was found
          AND di.dmd_ingredient_code IS NULL
        ORDER BY dc.ddd_comment, dc.refers_to_ingredient
        """

        results = client.query(validation_query).result()
        validation_issues = [dict(row) for row in results]

        if validation_issues:
            logger.warning(f"Found {len(validation_issues)} 'refers to' comments that could not be matched to dm+d ingredients:")
            for issue in validation_issues:
                logger.warning(f"  - Comment: '{issue['ddd_comment']}' -> Ingredient: '{issue['refers_to_ingredient']}'")
            return False
        else:
            logger.info("All 'refers to' comments successfully matched to dm+d ingredients.")
            return True

    except Exception as e:
        logger.error(f"Error during DDD 'refers to' validation: {str(e)}")
        raise


@flow(name="Populate DDD Refers To Table")
def populate_ddd_refers_to_table():
    logger = get_run_logger()
    logger.info("Populating DDD Refers To table")

    sql_file_path = Path(__file__).parent / "populate_ddd_refers_to.sql"

    sql_result = execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("DDD Refers To table populated successfully")

    schema_validation = validate_table_schema(DDD_REFERS_TO_TABLE_SPEC)
    logger.info("DDD Refers To table schema validation completed")

    validation_result = validate_ddd_refers_to_matches()
    if not validation_result:
        logger.warning("DDD 'refers to' validation found unmatched ingredients")
    else:
        logger.info("DDD 'refers to' validation passed - all comments successfully matched")
    

if __name__ == "__main__":
    populate_ddd_refers_to_table() 