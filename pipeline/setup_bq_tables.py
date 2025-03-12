from prefect import flow, task, get_run_logger
from google.cloud import bigquery
from google.api_core import exceptions

from bq_tables import (
    ORGANISATION_TABLE_SPEC,
    SCMD_RAW_TABLE_SPEC,
    SCMD_DATA_STATUS_TABLE_SPEC,
    SCMD_PROCESSED_TABLE_SPEC,
    UNITS_CONVERSION_TABLE_SPEC,
    ORG_AE_STATUS_TABLE_SPEC,
    DMD_TABLE_SPEC,
    DMD_SUPP_TABLE_SPEC,
    WHO_ATC_TABLE_SPEC,
    WHO_DDD_TABLE_SPEC,
    ADM_ROUTE_MAPPING_TABLE_SPEC,
    DOSE_TABLE_SPEC,
    INGREDIENT_QUANTITY_TABLE_SPEC,
    DDD_QUANTITY_TABLE_SPEC,
    WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC,
    VMP_DDD_MAPPING_TABLE_SPEC
)
from utils import get_bigquery_client

@task(cache_policy=None)
def create_table_if_not_exists(client: bigquery.Client, table_spec):
    """Create a BigQuery table if it doesn't exist."""
    logger = get_run_logger()
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
    except exceptions.NotFound:
        table = client.create_table(table)
        logger.info(f"Created table {table_id}")

@flow(name="setup_bigquery_tables")
def setup_tables():
    """Create all required BigQuery tables if they don't exist."""
    client = get_bigquery_client()
    create_table_if_not_exists(client, ORGANISATION_TABLE_SPEC)
    create_table_if_not_exists(client, SCMD_RAW_TABLE_SPEC)
    create_table_if_not_exists(client, SCMD_PROCESSED_TABLE_SPEC)
    create_table_if_not_exists(client, SCMD_DATA_STATUS_TABLE_SPEC)
    create_table_if_not_exists(client, UNITS_CONVERSION_TABLE_SPEC)
    create_table_if_not_exists(client, ORG_AE_STATUS_TABLE_SPEC)
    create_table_if_not_exists(client, DMD_TABLE_SPEC)
    create_table_if_not_exists(client, DMD_SUPP_TABLE_SPEC)
    create_table_if_not_exists(client, WHO_ATC_TABLE_SPEC)
    create_table_if_not_exists(client, WHO_DDD_TABLE_SPEC)
    create_table_if_not_exists(client, SCMD_PROCESSED_TABLE_SPEC)
    create_table_if_not_exists(client, ADM_ROUTE_MAPPING_TABLE_SPEC)
    create_table_if_not_exists(client, DOSE_TABLE_SPEC)
    create_table_if_not_exists(client, INGREDIENT_QUANTITY_TABLE_SPEC)
    create_table_if_not_exists(client, DDD_QUANTITY_TABLE_SPEC)
    create_table_if_not_exists(client, WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC)
    create_table_if_not_exists(client, VMP_DDD_MAPPING_TABLE_SPEC)

if __name__ == "__main__":
    setup_tables()

