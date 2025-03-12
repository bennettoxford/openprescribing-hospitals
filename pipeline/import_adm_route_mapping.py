from google.cloud import bigquery
from typing import Dict, List
from prefect import task, flow, get_run_logger
from config import PROJECT_ID, DATASET_ID, ADM_ROUTE_MAPPING_TABLE_ID, WHO_ROUTES_OF_ADMINISTRATION_TABLE_ID
from utils import get_bigquery_client


@task
def create_who_routes_of_administration():
    who_routes_of_administration = {
        "O": "Oral",
        "P": "Parenteral",
        "R": "Rectal",
        "SL": "Sublingual",
        "N": "Nasal",
        "TD": "Transdermal",
        "V": "Vaginal",
        "Inhal.solution": "Inhalation (solution)",
        "Inhal.powder": "Inhalation (powder)",
        "Inhal.aerosol": "Inhalation (aerosol)",
        "Instill.solution": "Instillation (solution)",
        "oral aerosol": "Oral (aerosol)",
        "Inhal": "Inhalation",
        "Chewing gum": "Chewing gum",
        "urethral": "Urethral",
        "implant": "Implant",
        "ointment": "Ointment",
        "lamella": "Lamella",
        "intravesical": "Intravesical",
        "s.c. implant": "Subcutaneous implant",
        "null": "Null"
    }

    return who_routes_of_administration

@task
def create_adm_route_mapping() -> Dict:
    dmd_who_route_mapping = {
        'Oral': 'O',
        'Intravenous': 'P',
        'Subcutaneous': 'P',
        'Intramuscular': 'P',
        'Inhalation': 'Inhal',
        'Rectal': 'R',
        'Transdermal': 'TD',
        'Route of administration not applicable': 'null',
        'Vaginal': 'V',
        'Ocular': 'P',
        'Nasal': 'N',
        'Intrathecal': 'P',
        'Sublingual': 'SL',
        'Buccal': 'SL',
        'Gastroenteral': 'O',
        'Oromucosal': 'SL',
        'Intraarticular': 'P',
        'Intraarterial': 'P',
        'Intracavernous': 'P',
        'Intralesional': 'P',
        'Intrabursal': 'P',
        'Cutaneous': 'P',
        'Epidural': 'P',
        'Intravesical': 'intravesical',
        'Intradermal': 'P',
        'Intracameral': 'P',
        'Periarticular': 'P',
        'Endotracheopulmonary': 'P',
        'Line lock': 'P',
        'Intraosseous': 'P',
        'Intravitreal': 'P',
        'Dental': 'O',
        'Intrapleural': 'P',
        'Intracardiac': 'P',
        'Intracoronary': 'P',
        'Gingival': 'SL',
        'Subconjunctival': 'P',
        'Urethral': 'urethral',
        'Intracerebroventricular': 'P',
        'Intraperitoneal': 'P',
        'Auricular': 'P',
        'Intestinal': 'P',
        'Extraamniotic': 'P',
        'Infiltration': 'P',
        'Regional perfusion': 'P',
        'Perineural': 'P',
        'Submucosal': 'P',
        'Body cavity': 'P',
        'Endocervical': 'V',
        'Endosinusial': 'N',
        'Epilesional': 'P',
        'Extracorporeal': 'P',
        'Haemodialysis': 'P',
        'Haemofiltration': 'P',
        'Implantation': 'implant',
        'Intracholangiopancreatic': 'P',
        'Intradiscal': 'P',
        'Intraepidermal': 'P',
        'Intraglandular': 'P',
        'Intralymphatic': 'P',
        'Inrtaocular': 'P',
        'Intraputaminal': 'P',
        'Intratumoral': 'P',
        'Intrauterine': 'P',
        'Peribulbar ocular': 'P',
        'Perilesional': 'P',
        'Peritumoral': 'P',
        'Submucosal rectal': 'R',
        'Subretinal': 'P',
        'Intracervical': 'P',
        'Intraocular': 'P'
    }

    return dmd_who_route_mapping


@task(cache_policy=None)
def import_who_routes_of_administration(client: bigquery.Client, table_id: str, route_mapping: Dict) -> List[Dict]:
    """Convert route mapping dictionary to a list of records"""
    logger = get_run_logger()
    route_records = []

    for who_route_code, who_route_description in route_mapping.items():
        record = {
            "who_route_code": who_route_code,
            "who_route_description": who_route_description
        }
        route_records.append(record)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info(f"Uploading {len(route_records)} route mapping records to BigQuery...")
    job = client.load_table_from_json(
        route_records,
        table_id,
        job_config=job_config
    )
    job.result()
    logger.info(f"Successfully imported {len(route_records)} route mapping records to {table_id}")


@task(cache_policy=None)
def import_adm_route_mapping(client: bigquery.Client, table_id: str, route_mapping: Dict) -> List[Dict]:
    """Convert route mapping dictionary to a list of records"""
    logger = get_run_logger()
    route_records = []

    for dmd_route, who_route_code in route_mapping.items():
        record = {
            "dmd_route": dmd_route,
            "who_route_code": who_route_code
        }
        route_records.append(record)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info(f"Uploading {len(route_records)} route mapping records to BigQuery...")
    job = client.load_table_from_json(
        route_records,
        table_id,
        job_config=job_config
    )
    job.result()  
    logger.info(f"Successfully imported {len(route_records)} route mapping records to {table_id}")

@flow(name="Import Administration Route Mapping")
def import_adm_route_mapping_flow():
    client = get_bigquery_client()
    who_routes_of_administration_table_id = f"{PROJECT_ID}.{DATASET_ID}.{WHO_ROUTES_OF_ADMINISTRATION_TABLE_ID}"
    adm_route_mapping_table_id = f"{PROJECT_ID}.{DATASET_ID}.{ADM_ROUTE_MAPPING_TABLE_ID}"
    
    who_routes_of_administration = create_who_routes_of_administration()
    import_who_routes_of_administration(client, who_routes_of_administration_table_id, who_routes_of_administration)

    adm_route_mapping = create_adm_route_mapping()
    import_adm_route_mapping(client, adm_route_mapping_table_id, adm_route_mapping)

if __name__ == "__main__":
    import_adm_route_mapping_flow()

