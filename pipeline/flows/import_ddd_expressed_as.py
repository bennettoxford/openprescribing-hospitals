from google.cloud import bigquery
from typing import Dict, List
from prefect import task, flow, get_run_logger
from pipeline.utils.config import PROJECT_ID, DATASET_ID
from pipeline.utils.utils import get_bigquery_client
from pipeline.bq_tables import VMP_EXPRESSED_AS_TABLE_SPEC


@task
def create_expressed_as_dict() -> Dict:
    expressed_as_dict = {
        "38893611000001108": {
            "vmp_name": (
                "Aclidinium bromide 375micrograms/dose dry powder inhaler"
            ),
            "ddd_comment": "Expressed as aclidinium, delivered dose",
            "expressed_as_strnt_nmrtr": 322,
            "expressed_as_strnt_nmrtr_uom": "258685003",
            "expressed_as_strnt_nmrtr_uom_name": "microgram",
        },
    }

    return expressed_as_dict


@task
def import_expressed_as(table_id: str, expressed_as_dict: Dict) -> List[Dict]:
    """Convert units dictionary to a list of records"""
    logger = get_run_logger()
    client = get_bigquery_client()

    expressed_as_records = []
    total_expressed_as = len(expressed_as_dict)
    logger.info(f"Processing {total_expressed_as} expressed as records")

    for i, (vmp_id, values) in enumerate(expressed_as_dict.items(), 1):
        vmp_name = values["vmp_name"]
        ddd_comment = values["ddd_comment"]
        expressed_as_strnt_nmrtr = values["expressed_as_strnt_nmrtr"]
        expressed_as_strnt_nmrtr_uom = values["expressed_as_strnt_nmrtr_uom"]
        expressed_as_strnt_nmrtr_uom_name = values[
            "expressed_as_strnt_nmrtr_uom_name"
        ]

        if vmp_id is not None:
            vmp_id = str(vmp_id)

        record = {
            "vmp_id": vmp_id,
            "vmp_name": vmp_name,
            "ddd_comment": ddd_comment,
            "expressed_as_strnt_nmrtr": expressed_as_strnt_nmrtr,
            "expressed_as_strnt_nmrtr_uom": expressed_as_strnt_nmrtr_uom,
            "expressed_as_strnt_nmrtr_uom_name": (
                expressed_as_strnt_nmrtr_uom_name
            ),
        }
        expressed_as_records.append(record)
        if i % 50 == 0 or i == total_expressed_as:
            progress_pct = (i / total_expressed_as) * 100
            logger.info(
                f"Progress: {i}/{total_expressed_as} expressed as records "
                f"processed ({progress_pct:.1f}%)"
            )

    job_config = bigquery.LoadJobConfig(
        schema=VMP_EXPRESSED_AS_TABLE_SPEC.schema,
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info("Uploading to BigQuery...")
    job = client.load_table_from_json(
        expressed_as_records, table_id, job_config=job_config
    )
    job.result()
    logger.info(
        f"Successfully imported {len(expressed_as_records)} expressed as "
        f"records to {table_id}"
    )


@task
def validate_expressed_as_units(expressed_as_dict: Dict) -> Dict:
    """Validate that unit IDs exist and unit names match"""
    logger = get_run_logger()
    client = get_bigquery_client()

    units_query = f"""
    SELECT unit_id, unit 
    FROM `{PROJECT_ID}.{DATASET_ID}.unit_conversion`
    WHERE unit_id IS NOT NULL
    """
    
    results = client.query(units_query).result()
    valid_units = {row.unit_id: row.unit for row in results}

    for vmp_id, data in expressed_as_dict.items():
        unit_id = data["expressed_as_strnt_nmrtr_uom"]
        unit_name = data["expressed_as_strnt_nmrtr_uom_name"]
        
        if unit_id not in valid_units:
            raise ValueError(f"Unit ID '{unit_id}' not found in units table")
            
        if valid_units[unit_id] != unit_name:
            raise ValueError(
                f"Unit name mismatch for {vmp_id}: "
                f"expected '{valid_units[unit_id]}' but got '{unit_name}'"
            )
    
    logger.info("All units validated successfully")
    return expressed_as_dict


@flow(name="Import Expressed As")
def import_expressed_as_flow():
    """Import expressed as data into BigQuery."""
    table_id = (
        f"{PROJECT_ID}.{DATASET_ID}.{VMP_EXPRESSED_AS_TABLE_SPEC.table_id}"
    )
    expressed_as_dict = create_expressed_as_dict()
    validated_dict = validate_expressed_as_units(expressed_as_dict)
    return import_expressed_as(table_id, validated_dict)


if __name__ == "__main__":
    import_expressed_as_flow()
