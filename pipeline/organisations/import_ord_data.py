import requests
import pandas as pd
import time
from google.cloud import bigquery
from typing import Dict, List, Any
from prefect import task, flow, get_run_logger

from pipeline.setup.config import PROJECT_ID, DATASET_ID, ORGANISATION_TABLE_ID
from pipeline.utils.utils import get_bigquery_client
from pipeline.setup.bq_tables import ORGANISATION_TABLE_SPEC


@task()
def fetch_all_trusts_from_ord() -> List[str]:
    """Fetch all organisation ODS codes for given roles."""
    logger = get_run_logger()
    roles = ["RO197", "RO24"]
    urls = [
        f"https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations?Roles={role}&Limit=1000"
        for role in roles
    ]
    all_orgs = []
    for url in urls:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()["Organisations"]
            all_orgs.extend([org["OrgId"] for org in data])
        except requests.RequestException as e:
            logger.error(f"Failed to fetch organisations: {str(e)}")
            raise e
    return all_orgs


@task()
def fetch_org_details(orgs: List[str]) -> Dict[str, Any]:
    """
    Get details for a list of organisations.

    Args:
        orgs (List[str]): The ODS codes of the organisations to get details for.

    Returns:
        Dict[str, Any]: A dictionary with the ODS codes as keys and the details as values.
    """
    logger = get_run_logger()
    all_orgs_details = {}
    total = len(orgs)
    logger.info(f"Fetching details for {total} organisations")

    for i, org in enumerate(orgs, 1):
        url = f"https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations/{org}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            all_orgs_details[org] = response.json()
            if i % 50 == 0 or i == total:
                logger.info(
                    f"Progress: {i}/{total} organisations processed ({(i/total)*100:.1f}%)"
                )
        except requests.RequestException as e:
            logger.error(f"Failed to fetch organisation details for {org}: {str(e)}")
            raise e
        time.sleep(0.2)
    return all_orgs_details


@task
def process_org_details(all_orgs_details: Dict[str, Any]) -> tuple:
    """Process organisation details and return successors and ICBs."""

    logger = get_run_logger()
    filtered_org_details = {
        org: data
        for org, data in all_orgs_details.items()
        if data["Organisation"]["GeoLoc"]["Location"]["Country"] == "ENGLAND"
    }

    predecessors = {}
    successors = {}
    icbs = {}

    for current_code, org_data in filtered_org_details.items():
        org = org_data["Organisation"]
        predecessors[current_code] = [
            succ["Target"]["OrgId"]["extension"]
            for succ in org.get("Succs", {}).get("Succ", [])
            if succ["Type"] == "Predecessor"
        ]
        successors[current_code] = [
            succ["Target"]["OrgId"]["extension"]
            for succ in org.get("Succs", {}).get("Succ", [])
            if succ["Type"] == "Successor"
        ]

        if "Rels" in org:
            for rel in org["Rels"]["Rel"]:
                if rel["id"] == "RE5":
                    icbs[current_code] = rel["Target"]["OrgId"]["extension"]

    # Successor relationship not present in ORD API relationship info.
    # RYK (Dudley Integrated Health and Care NHS Trust) was dissolved Oct 2024;
    # staff, property, services and liabilities transferred to TAJ (Black Country
    # Healthcare NHS Foundation Trust).
    # Ref: https://www.england.nhs.uk/publication/dudley-and-walsall-mental-health-partnership-nhs-trust/
    successors["RYK"] = list(dict.fromkeys(successors.get("RYK", []) + ["TAJ"]))
    predecessors["TAJ"] = list(dict.fromkeys(predecessors.get("TAJ", []) + ["RYK"]))

    logger.info(f"Found {len(icbs)} ICBs")

    return icbs, successors, predecessors, filtered_org_details


@task()
def get_icb_regions(icbs: List[str]) -> Dict[str, str]:
    """Get regions for ICBs."""
    logger = get_run_logger()
    icb_regions = {}
    icb_details = fetch_org_details(icbs)

    for icb, details in icb_details.items():
        rels = details.get("Organisation", {}).get("Rels", {}).get("Rel", [])
        for rel in rels:
            if rel["id"] == "RE2":
                region = rel["Target"]["OrgId"]["extension"]
                icb_regions[icb] = region
    logger.info(f"Found {len(icb_regions)} ICB regions")
    return icb_regions


@task()
def get_org_names(org_codes: set) -> Dict[str, str]:
    """Get names for organisations."""
    logger = get_run_logger()
    org_names = {}
    org_details = fetch_org_details(list(org_codes))

    for org_code, details in org_details.items():
        org_name = details.get("Organisation", {}).get("Name", "")
        if org_name:
            org_names[org_code] = format_org_name(org_name)
    logger.info(f"Found {len(org_names)} organisation names")
    return org_names


def format_org_name(name: str) -> str:
    """Format organisation name."""
    if not name:
        return name

    acronyms = {"NHS"}
    drop_strings = {"COMMISSIONING REGION", "INTEGRATED CARE BOARD"}

    for drop_string in drop_strings:
        name = name.replace(drop_string, "")

    words = name.split()
    formatted_words = [word if word in acronyms else word.title() for word in words]
    formatted_name = " ".join(formatted_words).strip()
    formatted_name = formatted_name.replace("'S ", "'s ").replace("'S,", "'s,")

    return formatted_name


@task
def upload_to_bigquery(df: pd.DataFrame, full_table_id: str) -> None:
    """Upload dataframe to BigQuery."""
    logger = get_run_logger()
    client = get_bigquery_client()

    date_columns = ['legal_closed_date', 'operational_closed_date', 'legal_open_date', 'operational_open_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    job_config = bigquery.LoadJobConfig(
        schema=ORGANISATION_TABLE_SPEC.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    logger.info(f"Imported {len(df)} organisation records to {full_table_id}")


@task
def create_org_mapping_df(
    successors: Dict[str, List[str]],
    predecessors: Dict[str, List[str]],
    orgs_details: Dict[str, Any],
    icbs: Dict[str, str],
    icb_regions: Dict[str, str],
    region_names: Dict[str, str],
    icb_names: Dict[str, str],
) -> pd.DataFrame:
    logger = get_run_logger()

    ultimate_successors = resolve_ultimate_successors(successors)

    columns = [
        "ods_code",
        "ods_name",
        "successors",
        "predecessors",
        "ultimate_successors",
        "legal_closed_date",
        "operational_closed_date",
        "legal_open_date",
        "operational_open_date",
        "postcode",
        "region_code",
        "region",
        "icb_code",
        "icb",
    ]
    org_mapping_df = pd.DataFrame(columns=columns)

    for org, org_details in orgs_details.items():
        icb = icbs.get(org, None)
        icb_name = icb_names.get(icb, None)
        region = icb_regions.get(icb, None)
        region_name = region_names.get(region, None)

        org_dates = org_details.get("Organisation", {}).get("Date", [])
        dates_dict = {}
        for date_entry in org_dates:
            date_type = date_entry.get("Type")
            if date_type:
                dates_dict[date_type] = {
                    "Start": date_entry.get("Start"),
                    "End": date_entry.get("End"),
                }

        org_details_dict = {
            "ods_code": org,
            "ods_name": format_org_name(
                org_details.get("Organisation", {}).get("Name", "")
            ),
            "successors": successors.get(org, None),
            "predecessors": predecessors.get(org, None),
            "ultimate_successors": ultimate_successors.get(org, None),
            "legal_closed_date": dates_dict.get("Legal", {}).get("End"),
            "operational_closed_date": dates_dict.get("Operational", {}).get("End"),
            "legal_open_date": dates_dict.get("Legal", {}).get("Start"),
            "operational_open_date": dates_dict.get("Operational", {}).get("Start"),
            "postcode": org_details.get("Organisation", {})
            .get("GeoLoc", {})
            .get("Location", {})
            .get("PostCode", None),
            "region_code": region,
            "region": region_name,
            "icb_code": icb,
            "icb": icb_name,
        }

        org_mapping_df = pd.concat(
            [org_mapping_df, pd.DataFrame([org_details_dict])], ignore_index=True
        )
    logger.info(f"Created {len(org_mapping_df)} organisation mapping records")
    return org_mapping_df


def resolve_ultimate_successors(successors_dict):
    """
    Resolve all succession chains to find ultimate successors.

    Args:
        successors_dict: Dictionary mapping ODS codes to their direct successors

    Returns:
        Dictionary mapping ODS codes to their ultimate successors
    """
    ultimate_successors = {}

    for org_code in successors_dict:

        visited = set()
        current_successors = successors_dict.get(org_code, [])
        final_successors = []

        for successor in current_successors:

            if successor not in successors_dict or not successors_dict[successor]:
                final_successors.append(successor)
                continue

            current = successor
            chain = [current]
            while current in successors_dict and successors_dict[current]:

                if current in visited:
                    break
                visited.add(current)

                next_successor = successors_dict[current][0]

                if next_successor in chain:
                    break

                chain.append(next_successor)
                current = next_successor

            if chain:
                final_successors.append(chain[-1])

        ultimate_successors[org_code] = final_successors

    return ultimate_successors


@flow(name="Import NHS Organisations")
def import_ord_data():
    """Main flow to import NHS organisation data."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{ORGANISATION_TABLE_ID}"

    all_orgs = fetch_all_trusts_from_ord()
    all_orgs_details = fetch_org_details(all_orgs)
    icbs, successors, predecessors, filtered_org_details = process_org_details(all_orgs_details)

    icbs_list = list(set(icbs.values()))
    icb_regions = get_icb_regions(icbs_list)

    unique_regions = set(icb_regions.values())
    region_names = get_org_names(unique_regions)
    icb_names = get_org_names(set(icbs_list))

    organisation_df = create_org_mapping_df(
        successors,
        predecessors,
        filtered_org_details,
        icbs,
        icb_regions,
        region_names,
        icb_names,
    )

    upload_result = upload_to_bigquery(organisation_df, table_id)



if __name__ == "__main__":
    import_ord_data()
