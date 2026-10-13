from pathlib import Path

from prefect import flow, get_run_logger, task

from pipeline.setup.bq_tables import DMD_FULL_TABLE_SPEC
from pipeline.utils.utils import (
    execute_bigquery_query_from_sql_file,
    get_bigquery_client,
    validate_table_schema,
)


def _failed_rows(table_id: str) -> dict[str, list[dict]]:
    client = get_bigquery_client()
    checks = {
        "vtm": f"""
            SELECT vmp_code, vmp_name
            FROM `{table_id}`
            WHERE vtm IS NOT NULL AND vtm_name IS NULL
        """,
        "ingredient_units": f"""
            SELECT vmp_code, vmp_name, ing.ing_name
            FROM `{table_id}`,
            UNNEST(ingredients) AS ing
            WHERE (ing.strnt_nmrtr_val IS NOT NULL AND ing.strnt_nmrtr_uom_name IS NULL)
            OR (ing.strnt_dnmtr_val IS NOT NULL AND ing.strnt_dnmtr_uom_name IS NULL)
        """,
        "basis_of_strength_base": f"""
            SELECT vmp_code, vmp_name, ing.ing_name, ing.basis_of_strength_type
            FROM `{table_id}`,
            UNNEST(ingredients) AS ing
            WHERE ing.basis_of_strength_type = 2
            AND (ing.basis_of_strength_code IS NULL OR ing.basis_of_strength_name IS NULL)
        """,
    }
    failed = {}
    for name, query in checks.items():
        rows = [dict(row) for row in client.query(query).result()]
        if rows:
            failed[name] = rows
    return failed


@task
def validate_dmd_full() -> None:
    logger = get_run_logger()
    if not validate_table_schema(DMD_FULL_TABLE_SPEC):
        raise ValueError(
            f"Schema validation failed for {DMD_FULL_TABLE_SPEC.full_table_id}"
        )

    failed = _failed_rows(DMD_FULL_TABLE_SPEC.full_table_id)
    if failed:
        logger.error(f"Validation failures: {failed}")
        raise ValueError(
            f"dm+d product validation failed for {DMD_FULL_TABLE_SPEC.full_table_id}"
        )

    logger.info("Full dm+d table validated")


@flow(name="Process dm+d full")
def process_dmd_full():
    """Build the full dm+d table from the raw TRUD tables."""
    logger = get_run_logger()
    logger.info("Building full dm+d table from raw TRUD tables")

    sql_file_path = Path(__file__).parent / "process_dmd_full.sql"
    execute_bigquery_query_from_sql_file(str(sql_file_path))
    logger.info("Full dm+d table built")
    validate_dmd_full()


if __name__ == "__main__":
    process_dmd_full()
