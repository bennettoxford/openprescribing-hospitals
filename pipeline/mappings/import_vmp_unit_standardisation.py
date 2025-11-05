from dataclasses import dataclass
from typing import Dict, List
from prefect import task, flow, get_run_logger
from google.cloud import bigquery

from pipeline.setup.config import (
    PROJECT_ID,
    DATASET_ID,
    VMP_UNIT_STANDARDISATION_TABLE_ID,
)
from pipeline.utils.utils import get_bigquery_client
from pipeline.setup.bq_tables import VMP_UNIT_STANDARDISATION_TABLE_SPEC


@dataclass
class UnitStandardisation:
    vmp_code: str
    vmp_name: str
    scmd_units: List[Dict[str, str]]
    chosen_unit_id: str
    chosen_unit_name: str
    conversion_logic: str
    conversion_factor: float


@task
def create_standardisation_dict() -> Dict[str, UnitStandardisation]:
    """
    Create dictionary of VMP unit standardisations.
    This is where you'll define the VMPs that need unit standardisation.
    """
    standardisations = {
        "37843911000001102": UnitStandardisation(
            vmp_code="37843911000001102",
            vmp_name="Cisplatin 40mg/250ml in Sodium chloride 0.9% infusion bags",
            scmd_units=[
                {"unit_id": "428672001", "unit_name": "bag"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 bag = 250 ml",
            conversion_factor=250.0
        ),
        "33626511000001102": UnitStandardisation(
            vmp_code="33626511000001102",
            vmp_name="Generic Dermatophagoides pteronyssinus 50,000SBU/ml solution for skin prick test",
            scmd_units=[
                {"unit_id": "3317411000001100", "unit_name": "dose"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 dose = 3 ml",
            conversion_factor=3.0
        ),
        "33517611000001101": UnitStandardisation(
            vmp_code="33517611000001101",
            vmp_name="Generic Oat (cultivated) (Avena sativa) pollen 50,000BU/ml solution for skin prick test",
            scmd_units=[
                {"unit_id": "3317411000001100", "unit_name": "dose"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 dose = 3 ml",
            conversion_factor=3
        ),
        "33526011000001100": UnitStandardisation(
            vmp_code="33526011000001100",
            vmp_name="Generic Birch pollen 50,000SBU/ml solution for skin prick test",
            scmd_units=[
                {"unit_id": "3317411000001100", "unit_name": "dose"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 dose = 3 ml",
            conversion_factor=3
        ),
        "33537911000001100": UnitStandardisation(
            vmp_code="33537911000001100",
            vmp_name="Generic Hazel (Corylus avellana) pollen 50,000BU/ml solution for skin prick test",
            scmd_units=[
                {"unit_id": "3317411000001100", "unit_name": "dose"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 dose = 3 ml",
            conversion_factor=3
        ),
        "33538311000001100": UnitStandardisation(
            vmp_code="33538311000001100",
            vmp_name="Generic Oak (Quercus robur) pollen 50,000BU/ml solution for skin prick test",
            scmd_units=[
                {"unit_id": "3317411000001100", "unit_name": "dose"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 dose = 3 ml",
            conversion_factor=3
        ),
        "33546011000001103": UnitStandardisation(
            vmp_code="33546011000001103",
            vmp_name="Generic Poplar (Populus alba) pollen 50,000BU/ml solution for skin prick test",
            scmd_units=[
                {"unit_id": "3317411000001100", "unit_name": "dose"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 dose = 3 ml",
            conversion_factor=3
        ),
        "33564811000001107": UnitStandardisation(
            vmp_code="33564811000001107",
            vmp_name="Generic Willow (Salix caprea) pollen 50,000BU/ml solution for skin prick test",
            scmd_units=[
                {"unit_id": "3317411000001100", "unit_name": "dose"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 dose = 3 ml",
            conversion_factor=3
        ),
        "33633611000001107": UnitStandardisation(
            vmp_code="33633611000001107",
            vmp_name="Generic Horse Epithelia 10,000BU/ml solution for skin prick test",
            scmd_units=[
                {"unit_id": "3317411000001100", "unit_name": "dose"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 dose = 3 ml",
            conversion_factor=3.0
        ),
        "33676411000001102": UnitStandardisation(
            vmp_code="33676411000001102",
            vmp_name="Generic Positive Control 0.1% Histamine solution for skin prick test",
            scmd_units=[
                {"unit_id": "3317411000001100", "unit_name": "dose"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 dose = 3 ml",
            conversion_factor=3.0
        ),
        "4217811000001106": UnitStandardisation(
            vmp_code="4217811000001106",
            vmp_name="Glycerol liquid",
            scmd_units=[
                {"unit_id": "258682000", "unit_name": "gram"},
                {"unit_id": "258773002", "unit_name": "ml"},
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 gram = 1 ml",
            conversion_factor=1.0
        ),
        "10638011000001105": UnitStandardisation(
            vmp_code="10638011000001105",
            vmp_name="Levothyroxine sodium 500microgram powder for solution for injection vials",
            scmd_units=[
                {"unit_id": "258685003", "unit_name": "microgram"},
                {"unit_id": "415818006", "unit_name": "vial"}
            ],
            chosen_unit_id="415818006",
            chosen_unit_name="vial",
            conversion_logic="1 vial = 500 micrograms",
            conversion_factor=1/500
        ),
        "7294111000001102": UnitStandardisation(
            vmp_code="7294111000001102",
            vmp_name="Ostomy deodorants",
            scmd_units=[
                {"unit_id": "258682000", "unit_name": "gram"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 gram = 1 ml",
            conversion_factor=1.0
        ),
        "38007911000001105": UnitStandardisation(
            vmp_code="38007911000001105",
            vmp_name="Voretigene neparvovec 5 tera vector genomes/1ml concentrate and solvent for solution for injection vials",
            scmd_units=[
                {"unit_id": "258773002", "unit_name": "ml"},
                {"unit_id": "415818006", "unit_name": "vial"},
            ],
            chosen_unit_id="415818006",
            chosen_unit_name="vial",
            conversion_logic="1 vial = 1ml",
            conversion_factor=1.0
        ),
        "39721111000001109": UnitStandardisation(
            vmp_code="39721111000001109",
            vmp_name="Zolmitriptan 5mg/0.1ml nasal spray unit dose",
            scmd_units=[
                {"unit_id": "258773002", "unit_name": "ml"},
                {"unit_id": "408102007", "unit_name": "unit dose"},
            ],
            chosen_unit_id="408102007",
            chosen_unit_name="unit dose",
            conversion_logic="1 ml = 10 unit doses",
            conversion_factor=10.0
        ),
        "10161711000001105": UnitStandardisation(
            vmp_code="10161711000001105",
            vmp_name="Levodopa 20mg/ml / Carbidopa monohydrate 5mg/ml intestinal gel 100ml cassette",
            scmd_units=[
                {"unit_id": "428672001", "unit_name": "bag"},
                {"unit_id": "10698211000001105", "unit_name": "cassette"}
            ],
            chosen_unit_id="10698211000001105",
            chosen_unit_name="cassette",
            conversion_logic="1 bag = 1 cassette",
            conversion_factor=1
        ),
        "17281111000001103": UnitStandardisation(
            vmp_code="17281111000001103",
            vmp_name="Romiplostim 250microgram powder and solvent for solution for injection vials",
            scmd_units=[
                {"unit_id": "3318611000001103", "unit_name": "pre-filled disposable injection"},
                {"unit_id": "415818006", "unit_name": "vial"}
            ],
            chosen_unit_id="415818006",
            chosen_unit_name="vial",
            conversion_logic="1 pre-filled disposable injection = 1 vial",
            conversion_factor=1
        ),
        "35099611000001102": UnitStandardisation(
            vmp_code="35099611000001102",
            vmp_name="Generic Custodiol solution for cardioplegia / organ preservation 1litre bags",
            scmd_units=[
                {"unit_id": "428672001", "unit_name": "bag"},
                {"unit_id": "258770004", "unit_name": "litre"}
            ],
            chosen_unit_id="258770004",
            chosen_unit_name="litre",
            conversion_logic="1 bag = 1 litre",
            conversion_factor=1
        ),
        "9062611000001102": UnitStandardisation(
            vmp_code="9062611000001102",
            vmp_name="Citric acid 1g/10ml oral solution unit dose sugar free",
            scmd_units=[
                {"unit_id": "733013000", "unit_name": "sachet"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 sachet = 10 ml",
            conversion_factor=10
        ),
        "34938511000001103": UnitStandardisation(
            vmp_code="34938511000001103",
            vmp_name="Measles, Mumps and Rubella vaccine (live) powder and solvent for solution for injection 0.5ml pre-filled syringes",
            scmd_units=[
                {"unit_id": "3318611000001103", "unit_name": "pre-filled disposable injection"},
                {"unit_id": "258773002", "unit_name": "ml"}
            ],
            chosen_unit_id="258773002",
            chosen_unit_name="ml",
            conversion_logic="1 pre-filled disposable injection = 0.5 ml",
            conversion_factor=0.5
        ),
    }
    
    return standardisations


@task
def import_standardisations(
    table_id: str, 
    standardisations: Dict[str, UnitStandardisation]
) -> None:
    """Import VMP unit standardisations into BigQuery"""
    logger = get_run_logger()
    client = get_bigquery_client()

    records = []
    total_vmps = len(standardisations)
    logger.info(f"Processing {total_vmps} VMP unit standardisations")

    for vmp_code, standardisation in standardisations.items():
        record = {
            "vmp_code": standardisation.vmp_code,
            "vmp_name": standardisation.vmp_name,
            "scmd_units": standardisation.scmd_units,
            "chosen_unit_id": standardisation.chosen_unit_id,
            "chosen_unit_name": standardisation.chosen_unit_name,
            "conversion_logic": standardisation.conversion_logic,
            "conversion_factor": standardisation.conversion_factor
        }
        records.append(record)

    job_config = bigquery.LoadJobConfig(
        schema=VMP_UNIT_STANDARDISATION_TABLE_SPEC.schema,
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info("Uploading to BigQuery...")
    job = client.load_table_from_json(
        records, 
        table_id, 
        job_config=job_config
    )
    job.result()
    logger.info(
        f"Successfully imported {len(records)} VMP unit standardisation records to {table_id}"
    )


@flow(name="Import VMP Unit Standardisation")
def import_vmp_unit_standardisation():
    """Import VMP unit standardisation data into BigQuery."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{VMP_UNIT_STANDARDISATION_TABLE_ID}"
    standardisations = create_standardisation_dict()
    import_standardisations(table_id, standardisations)


if __name__ == "__main__":
    import_vmp_unit_standardisation()
