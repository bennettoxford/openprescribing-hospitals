from google.cloud import bigquery
from typing import Dict, List
from prefect import task, flow, get_run_logger
from pipeline.setup.config import PROJECT_ID, DATASET_ID, UNITS_CONVERSION_TABLE_ID
from pipeline.utils.utils import get_bigquery_client
from pipeline.setup.bq_tables import UNITS_CONVERSION_TABLE_SPEC


@task
def create_units_dict() -> Dict:
    units_dict = {
        "actuation": {
            "basis": "actuation",
            "conversion_factor": 1,
            "id": "732981002",
            "basis_id": "732981002",
        },
        "ampoule": {
            "basis": "ampoule",
            "conversion_factor": 1,
            "id": "413516001",
            "basis_id": "413516001",
        },
        "application": {
            "basis": "application",
            "conversion_factor": 1,
            "id": "10692211000001108",
            "basis_id": "10692211000001108",
        },
        "applicator": {
            "basis": "applicator",
            "conversion_factor": 1,
            "id": "732980001",
            "basis_id": "732980001",
        },
        "bag": {
            "basis": "bag",
            "conversion_factor": 1,
            "id": "428672001",
            "basis_id": "428672001",
        },
        "bandage": {
            "basis": "bandage",
            "conversion_factor": 1,
            "id": "8091811000001107",
            "basis_id": "8091811000001107",
        },
        "bottle": {
            "basis": "bottle",
            "conversion_factor": 1,
            "id": "419672006",
            "basis_id": "419672006",
        },
        "can": {
            "basis": "can",
            "conversion_factor": 1,
            "id": "429308001",
            "basis_id": "429308001",
        },
        "capsule": {
            "basis": "capsule",
            "conversion_factor": 1,
            "id": "428641000",
            "basis_id": "428641000",
        },
        "cartridge": {
            "basis": "cartridge",
            "conversion_factor": 1,
            "id": "732988008",
            "basis_id": "732988008",
        },
        "cassette": {
            "basis": "cassette",
            "conversion_factor": 1,
            "id": "10698211000001105",
            "basis_id": "10698211000001105",
        },
        "catheter": {
            "basis": "catheter",
            "conversion_factor": 1,
            "id": "3319911000001101",
            "basis_id": "3319911000001101",
        },
        "container": {
            "basis": "container",
            "conversion_factor": 1,
            "id": "732990009",
            "basis_id": "732990009",
        },
        "cup": {
            "basis": "cup",
            "conversion_factor": 1,
            "id": "428178003",
            "basis_id": "428178003",
        },
        "device": {
            "basis": "device",
            "conversion_factor": 1,
            "id": "3318711000001107",
            "basis_id": "3318711000001107",
        },
        "dose": {
            "basis": "dose",
            "conversion_factor": 1,
            "id": "3317411000001100",
            "basis_id": "3317411000001100",
        },
        "dressing": {
            "basis": "dressing",
            "conversion_factor": 1,
            "id": "3320111000001103",
            "basis_id": "3320111000001103",
        },
        "drop": {
            "basis": "drop",
            "conversion_factor": 1,
            "id": "10693611000001100",
            "basis_id": "10693611000001100",
        },
        "elastomeric device": {
            "basis": "elastomeric device",
            "conversion_factor": 1,
            "id": "10696111000001103",
            "basis_id": "10696111000001103",
        },
        "enema": {
            "basis": "enema",
            "conversion_factor": 1,
            "id": "700476008",
            "basis_id": "700476008",
        },
        "film": {
            "basis": "film",
            "conversion_factor": 1,
            "id": "732995004",
            "basis_id": "732995004",
        },
        "gbq": {
            "basis": "kbq",
            "conversion_factor": 1000000,
            "id": "418931004",
            "basis_id": "282143001",
        },
        "generator": {
            "basis": "generator",
            "conversion_factor": 1,
            "id": "8091011000001101",
            "basis_id": "8091011000001101",
        },
        "giga vector genome": {
            "basis": "vector genome",
            "conversion_factor": 1000000000,
            "id": "10697811000001107",
            "basis_id": "10696211000001109",
        },
        "glove": {
            "basis": "glove",
            "conversion_factor": 1,
            "id": "3321511000001100",
            "basis_id": "3321511000001100",
        },
        # WHO
        "g": {
            "basis": "gram",
            "conversion_factor": 1,
            "id": None,
            "basis_id": "258682000",
        },
        "gram": {
            "basis": "gram",
            "conversion_factor": 1,
            "id": "258682000",
            "basis_id": "258682000",
        },
        "hour": {
            "basis": "hour",
            "conversion_factor": 1,
            "id": "258702006",
            "basis_id": "258702006",
        },
        "insert": {
            "basis": "insert",
            "conversion_factor": 1,
            "id": "428639001",
            "basis_id": "428639001",
        },
        "kallikrein inactivator unit": {
            "basis": "kallikrein inactivator unit",
            "conversion_factor": 1,
            "id": "411225003",
            "basis_id": "411225003",
        },
        "kbq": {
            "basis": "kbq",
            "conversion_factor": 1,
            "id": "282143001",
            "basis_id": "282143001",
        },
        "kit": {
            "basis": "kit",
            "conversion_factor": 1,
            "id": "419179001",
            "basis_id": "419179001",
        },
        "lancet": {
            "basis": "lancet",
            "conversion_factor": 1,
            "id": "3317611000001102",
            "basis_id": "3317611000001102",
        },
        "larva": {
            "basis": "larva",
            "conversion_factor": 1,
            "id": "8090511000001102",
            "basis_id": "8090511000001102",
        },
        "leech": {
            "basis": "leech",
            "conversion_factor": 1,
            "id": "10693811000001101",
            "basis_id": "10693811000001101",
        },
        "litre": {
            "basis": "ml",
            "conversion_factor": 1000,
            "id": "258770004",
            "basis_id": "258773002",
        },
        "lozenge": {
            "basis": "lozenge",
            "conversion_factor": 1,
            "id": "429587008",
            "basis_id": "429587008",
        },
        # WHO
        "lsu": {
            "basis": "lsu",
            "conversion_factor": 1,
            "id": None,
            "basis_id": None,
        },
        "m": {
            "basis": "m",
            "conversion_factor": 1,
            "id": "258669008",
            "basis_id": "258669008",
        },
        "mbq": {
            "basis": "kbq",
            "conversion_factor": 1000,
            "id": "229034000",
            "basis_id": "282143001",
        },
        "mega unit": {
            "basis": "mega unit",
            "conversion_factor": 1,
            "id": "408165007",
            "basis_id": "408165007",
        },
        "mg": {
            "basis": "gram",
            "conversion_factor": 0.001,
            "id": "258684004",
            "basis_id": "258682000",
        },
        # WHO
        "mcg": {
            "basis": "gram",
            "conversion_factor": 1e-06,
            "id": None,
            "basis_id": "258682000",
        },
        "microgram": {
            "basis": "gram",
            "conversion_factor": 1e-06,
            "id": "258685003",
            "basis_id": "258682000",
        },
        "microlitre": {
            "basis": "ml",
            "conversion_factor": 0.001,
            "id": "258774008",
            "basis_id": "258773002",
        },
        "micromol": {
            "basis": "mmol",
            "conversion_factor": 0.001,
            "id": "258719008",
            "basis_id": "258718000",
        },
        "million colony forming units": {
            "basis": "colony forming unit",
            "conversion_factor": 1000000,
            "id": "10698011000001100",
            "basis_id": "10698011000001100",
        },
        "million plaque forming units": {
            "basis": "plaque forming unit",
            "conversion_factor": 1000000,
            "id": "10695711000001105",
            "basis_id": "10695711000001105",
        },
        "ml": {
            "basis": "ml",
            "conversion_factor": 1,
            "id": "258773002",
            "basis_id": "258773002",
        },
        "mmol": {
            "basis": "mmol",
            "conversion_factor": 1,
            "id": "258718000",
            "basis_id": "258718000",
        },
        # WHO
        "mu": {
            "basis": "unit",
            "conversion_factor": 1000000,
            "id": None,
            "basis_id": "767525000",
        },
        "nanogram": {
            "basis": "gram",
            "conversion_factor": 1e-09,
            "id": "258686002",
            "basis_id": "258682000",
        },
        "nanolitre": {
            "basis": "ml",
            "conversion_factor": 0.000001,
            "id": "282113003",
            "basis_id": "258773002",
        },
        "needle": {
            "basis": "needle",
            "conversion_factor": 1,
            "id": "3318111000001106",
            "basis_id": "3318111000001106",
        },
        "pack": {
            "basis": "pack",
            "conversion_factor": 1,
            "id": "3318211000001100",
            "basis_id": "3318211000001100",
        },
        "pad": {
            "basis": "pad",
            "conversion_factor": 1,
            "id": "421823004",
            "basis_id": "421823004",
        },
        "pastille": {
            "basis": "pastille",
            "conversion_factor": 1,
            "id": "3318311000001108",
            "basis_id": "3318311000001108",
        },
        "patch": {
            "basis": "patch",
            "conversion_factor": 1,
            "id": "419702001",
            "basis_id": "419702001",
        },
        "pessary": {
            "basis": "pessary",
            "conversion_factor": 1,
            "id": "733007009",
            "basis_id": "733007009",
        },
        "piece": {
            "basis": "piece",
            "conversion_factor": 1,
            "id": "3321411000001104",
            "basis_id": "3321411000001104",
        },
        "plaster": {
            "basis": "plaster",
            "conversion_factor": 1,
            "id": "733010002",
            "basis_id": "733010002",
        },
        "pot": {
            "basis": "pot",
            "conversion_factor": 1,
            "id": "3321111000001109",
            "basis_id": "3321111000001109",
        },
        "pouch": {
            "basis": "pouch",
            "conversion_factor": 1,
            "id": "733012005",
            "basis_id": "733012005",
        },
        "ppm": {
            "basis": "ppm",
            "conversion_factor": 1,
            "id": "258731005",
            "basis_id": "258731005",
        },
        "pre-filled disposable injection": {
            "basis": "pre-filled disposable injection",
            "conversion_factor": 1,
            "id": "3318611000001103",
            "basis_id": "3318611000001103",
        },
        "sachet": {
            "basis": "sachet",
            "conversion_factor": 1,
            "id": "733013000",
            "basis_id": "733013000",
        },
        "spoonful": {
            "basis": "spoonful",
            "conversion_factor": 1,
            "id": "733015007",
            "basis_id": "733015007",
        },
        "sq-bet": {
            "basis": "sq-bet",
            "conversion_factor": 1,
            "id": "10697511000001109",
            "basis_id": "10697511000001109",
        },
        "sq-t": {
            "basis": "sq-t",
            "conversion_factor": 1,
            "id": "10693011000001107",
            "basis_id": "10693011000001107",
        },
        "sq-u": {
            "basis": "sq-u",
            "conversion_factor": 1,
            "id": "10697111000001100",
            "basis_id": "10697111000001100",
        },
        "square cm": {
            "basis": "m",
            "conversion_factor": 0.01,
            "id": "259022006",
            "basis_id": "258669008",
        },
        "stocking": {
            "basis": "stocking",
            "conversion_factor": 1,
            "id": "3319011000001100",
            "basis_id": "3319011000001100",
        },
        "strip": {
            "basis": "strip",
            "conversion_factor": 1,
            "id": "733018009",
            "basis_id": "733018009",
        },
        "suppository": {
            "basis": "suppository",
            "conversion_factor": 1,
            "id": "430293001",
            "basis_id": "430293001",
        },
        "suture": {
            "basis": "suture",
            "conversion_factor": 1,
            "id": "3320311000001101",
            "basis_id": "3320311000001101",
        },
        "swab": {
            "basis": "swab",
            "conversion_factor": 1,
            "id": "420401004",
            "basis_id": "420401004",
        },
        "syringe": {
            "basis": "syringe",
            "conversion_factor": 1,
            "id": "733020007",
            "basis_id": "733020007",
        },
        "system": {
            "basis": "system",
            "conversion_factor": 1,
            "id": "733021006",
            "basis_id": "733021006",
        },
        "tablet": {
            "basis": "tablet",
            "conversion_factor": 1,
            "id": "428673006",
            "basis_id": "428673006",
        },
        "tera vector genome": {
            "basis": "vector genome",
            "conversion_factor": 1000000000000,
            "id": "10696711000001102",
            "basis_id": "10696211000001109",
        },
        "truss": {
            "basis": "truss",
            "conversion_factor": 1,
            "id": "3320411000001108",
            "basis_id": "3320411000001108",
        },
        # WHO
        "tu": {
            "basis": "unit",
            "conversion_factor": 1000,
            "id": None,
            "basis_id": "767525000",
        },
        "tube": {
            "basis": "tube",
            "conversion_factor": 1,
            "id": "418530008",
            "basis_id": "418530008",
        },
        "tuberculin unit": {
            "basis": "tuberculin unit",
            "conversion_factor": 1,
            "id": "415758003",
            "basis_id": "415758003",
        },
        # WHO
        "u": {
            "basis": "unit",
            "conversion_factor": 1,
            "id": None,
            "basis_id": "767525000",
        },
        "unit": {
            "basis": "unit",
            "conversion_factor": 1,
            "id": "767525000",
            "basis_id": "767525000",
        },
        "unit dose": {
            "basis": "unit dose",
            "conversion_factor": 1,
            "id": "408102007",
            "basis_id": "408102007",
        },
        "unit/dose": {
            "basis": "unit/dose",
            "conversion_factor": 1,
            "id": "10692011000001103",
            "basis_id": "10692011000001103",
        },
        "vial": {
            "basis": "vial",
            "conversion_factor": 1,
            "id": "415818006",
            "basis_id": "415818006",
        },
    }

    return units_dict


@task
def import_mapping(table_id: str, units_dict: Dict) -> List[Dict]:
    """Convert units dictionary to a list of records"""
    logger = get_run_logger()
    client = get_bigquery_client()

    conversion_records = []
    total_units = len(units_dict)
    logger.info(f"Processing {total_units} unit conversions")

    for i, (unit, values) in enumerate(units_dict.items(), 1):
        unit_id = values["id"]
        basis_id = values["basis_id"]

        if unit_id is not None:
            unit_id = str(unit_id)

        if basis_id is not None:
            basis_id = str(basis_id)

        record = {
            "unit": unit,
            "basis": values["basis"],
            "conversion_factor": float(values["conversion_factor"]),
            "unit_id": unit_id,
            "basis_id": basis_id,
        }
        conversion_records.append(record)
        if i % 50 == 0 or i == total_units:
            logger.info(
                f"Progress: {i}/{total_units} units processed ({(i/total_units)*100:.1f}%)"
            )

    job_config = bigquery.LoadJobConfig(
        schema=UNITS_CONVERSION_TABLE_SPEC.schema,
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info("Uploading to BigQuery...")
    job = client.load_table_from_json(
        conversion_records, table_id, job_config=job_config
    )
    job.result()
    logger.info(
        f"Successfully imported {len(conversion_records)} unit conversion records to {table_id}"
    )


@flow(name="Import Unit Conversion")
def import_unit_conversion():
    """Import unit conversion data into BigQuery."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{UNITS_CONVERSION_TABLE_ID}"
    units_dict = create_units_dict()
    import_mapping(table_id, units_dict)


if __name__ == "__main__":
    import_unit_conversion()
