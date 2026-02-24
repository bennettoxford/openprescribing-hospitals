from google.cloud import bigquery
from prefect import flow, get_run_logger

from pipeline.setup.bq_tables import VMP_ATC_MANUAL_TABLE_SPEC
from pipeline.utils.utils import get_bigquery_client

# Manual VMP-ATC mappings for products without a matched ATC code in dm+d supplementary.
VMP_ATC_MANUAL_MAPPINGS = [
    {
        "vmp_code": "31007811000001109",
        "vmp_name": "Ceftolozane 1g / Tazobactam 500mg powder for solution for infusion vials",
        "atc_code": "J01DI54"
    },
    {
        "vmp_code": "38840011000001101",
        "vmp_name": "Generic Recarbrio 500mg/500mg/250mg powder for solution for infusion vials",
        "atc_code": "J01DH56"
    },
]


@flow(name="Import VMP ATC Manual Mappings")
def import_vmp_atc_manual():
    """Load manual VMP-ATC mappings into BigQuery."""
    logger = get_run_logger()
    client = get_bigquery_client()

    records = [
        {
            "vmp_code": str(r["vmp_code"]),
            "vmp_name": str(r["vmp_name"]) if r.get("vmp_name") else None,
            "atc_code": str(r["atc_code"]),
        }
        for r in VMP_ATC_MANUAL_MAPPINGS
    ]

    job_config = bigquery.LoadJobConfig(
        schema=VMP_ATC_MANUAL_TABLE_SPEC.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    logger.info(
        f"Loading {len(records)} manual VMP-ATC mappings to {VMP_ATC_MANUAL_TABLE_SPEC.full_table_id}"
    )
    job = client.load_table_from_json(
        records, VMP_ATC_MANUAL_TABLE_SPEC.full_table_id, job_config=job_config
    )
    job.result()
    logger.info(
        f"Successfully loaded {len(records)} records to {VMP_ATC_MANUAL_TABLE_SPEC.full_table_id}"
    )


if __name__ == "__main__":
    import_vmp_atc_manual()
