from prefect import flow, task, get_run_logger
from google.cloud import bigquery
from google.api_core import exceptions

from pipeline.setup.bq_tables import (
    ORGANISATION_TABLE_SPEC,
    SCMD_RAW_PROVISIONAL_TABLE_SPEC,
    SCMD_RAW_FINALISED_TABLE_SPEC,
    SCMD_PROCESSED_TABLE_SPEC,
    SCMD_DATA_STATUS_TABLE_SPEC,
    UNITS_CONVERSION_TABLE_SPEC,
    ORG_AE_STATUS_TABLE_SPEC,
    DMD_TABLE_SPEC,
    DMD_FULL_TABLE_SPEC,
    DMD_SUPP_TABLE_SPEC,
    VMP_ATC_MANUAL_TABLE_SPEC,
    WHO_ATC_TABLE_SPEC,
    WHO_DDD_TABLE_SPEC,
    AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC,
    ADM_ROUTE_MAPPING_TABLE_SPEC,
    DOSE_TABLE_SPEC,
    INGREDIENT_QUANTITY_TABLE_SPEC,
    DDD_QUANTITY_TABLE_SPEC,
    WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC,
    VMP_TABLE_SPEC,
    VMP_UNIT_STANDARDISATION_TABLE_SPEC,
    VTM_INGREDIENTS_TABLE_SPEC,
    DMD_HISTORY_TABLE_SPEC,
    DMD_UOM_TABLE_SPEC,
    WHO_DDD_ALTERATIONS_TABLE_SPEC,
    WHO_ATC_ALTERATIONS_TABLE_SPEC,
    DDD_REFERS_TO_TABLE_SPEC,
    ERIC_TRUST_DATA_TABLE_SPEC,
    DOSE_CALCULATION_LOGIC_TABLE_SPEC,
    INGREDIENT_CALCULATION_LOGIC_TABLE_SPEC,
    DDD_CALCULATION_LOGIC_TABLE_SPEC,
    VMP_EXPRESSED_AS_TABLE_SPEC
)
from pipeline.utils.utils import get_bigquery_client


@task()
def create_table_if_not_exists(table_spec):
    """Create a BigQuery table if it doesn't exist."""
    logger = get_run_logger()
    client = get_bigquery_client()
    table_id = table_spec.full_table_id

    table = bigquery.Table(table_id, schema=table_spec.schema)
    table.description = table_spec.description

    if table_spec.partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            field=table_spec.partition_field
        )

    if table_spec.cluster_fields:
        table.clustering_fields = table_spec.cluster_fields

    try:
        client.get_table(table_id)
        logger.info(f"Table {table_id} already exists")
        return {"table_id": table_id, "status": "exists"}
    except exceptions.NotFound:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")
        return {"table_id": table_id, "status": "created"}


@flow(name="setup_bigquery_tables")
def setup_tables():
    """Create all required BigQuery tables if they don't exist."""
    tables_list = [
        ORGANISATION_TABLE_SPEC,
        SCMD_DATA_STATUS_TABLE_SPEC,
        SCMD_RAW_PROVISIONAL_TABLE_SPEC,
        SCMD_RAW_FINALISED_TABLE_SPEC,
        SCMD_PROCESSED_TABLE_SPEC,
        UNITS_CONVERSION_TABLE_SPEC,
        ORG_AE_STATUS_TABLE_SPEC,
        DMD_TABLE_SPEC,
        DMD_FULL_TABLE_SPEC,
        DMD_SUPP_TABLE_SPEC,
        VMP_ATC_MANUAL_TABLE_SPEC,
        WHO_ATC_TABLE_SPEC,
        WHO_DDD_TABLE_SPEC,
        AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC,
        ADM_ROUTE_MAPPING_TABLE_SPEC,
        DOSE_TABLE_SPEC,
        INGREDIENT_QUANTITY_TABLE_SPEC,
        DDD_QUANTITY_TABLE_SPEC,
        WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC,
        VMP_TABLE_SPEC,
        VMP_UNIT_STANDARDISATION_TABLE_SPEC,
        VTM_INGREDIENTS_TABLE_SPEC,
        DMD_HISTORY_TABLE_SPEC,
        DMD_UOM_TABLE_SPEC,
        WHO_DDD_ALTERATIONS_TABLE_SPEC,
        WHO_ATC_ALTERATIONS_TABLE_SPEC,
        DDD_REFERS_TO_TABLE_SPEC,
        ERIC_TRUST_DATA_TABLE_SPEC,
        DOSE_CALCULATION_LOGIC_TABLE_SPEC,
        INGREDIENT_CALCULATION_LOGIC_TABLE_SPEC,
        DDD_CALCULATION_LOGIC_TABLE_SPEC,
        VMP_EXPRESSED_AS_TABLE_SPEC
    ]


    for table in tables_list:
        create_table_if_not_exists(table)


if __name__ == "__main__":
    setup_tables()
