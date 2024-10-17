import requests
import pandas as pd
import time

from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from tqdm import tqdm
from typing import Dict, List, Any
from django.core.management.base import BaseCommand

from viewer.management.utils import get_bigquery_client

PROJECT_ROOT = Path(__file__).parent.parent
PROJECT_ID = "ebmdatalab"
DATASET_ID = "scmd"
TABLE_ID = "ods_mapped"
BASE_URL = "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations"
ROLES = ["RO197", "RO24"]  # NHS TRUST, NHS TRUST DERIVED

class Command(BaseCommand):
    help = "Imports organisation data into BigQuery"

    def handle(self, *args, **options):
        all_orgs = get_all_orgs(ROLES)
        self.stdout.write(f"Found {len(all_orgs)} organisations")

        all_orgs_details = get_org_details(all_orgs)

        names, predecessors, icbs = process_org_details(all_orgs_details)

        org_mapping_df = create_org_mapping_df(predecessors, all_orgs_details, names, icbs)

        icbs_list = org_mapping_df["icb"].dropna().unique().tolist()
        icb_regions = get_icb_regions(icbs_list)
        unique_regions = set(icb_regions.values())
        region_names = get_region_names(unique_regions)

        org_mapping_df["region"] = org_mapping_df["icb"].map(icb_regions).map(region_names)
        org_mapping_df.loc[org_mapping_df["ods_name"].notna(), "ods_name"] = org_mapping_df.loc[org_mapping_df["ods_name"].notna(), "ods_name"].apply(format_org_name)
        org_mapping_df.loc[org_mapping_df["region"].notna(), "region"] = org_mapping_df.loc[org_mapping_df["region"].notna(), "region"].apply(format_org_name)
        
        upload_bq(org_mapping_df)
        self.stdout.write(self.style.SUCCESS("Successfully imported organisation data into BigQuery"))

# The rest of the functions (get_all_orgs, get_org_details, process_org_details, etc.) remain the same

def get_all_orgs(roles) -> List[str]:
    """
    Get a list of ODS codes for all organisations with the specified roles.

    Args:
        roles (List[str]): The roles to filter by.

    Returns:
        List[str]: A list of ODS codes for all organisations with the specified roles.
    """
    urls = [f"{BASE_URL}?Roles={role}&Limit=1000" for role in roles]
    all_orgs = []
    for url in urls:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()["Organisations"]
            all_orgs.extend([org["OrgId"] for org in data])
        except requests.RequestException as e:
            print(f"Error fetching organizations: {e}")
    return all_orgs


def get_org_details(orgs: List[str]) -> Dict[str, Any]:
    """
    Get details for a list of organisations.

    Args:
        orgs (List[str]): The ODS codes of the organisations to get details for.

    Returns:
        Dict[str, Any]: A dictionary with the ODS codes as keys and the details as values.
    """
    all_orgs_details = {}
    for org in tqdm(orgs):
        url = f"{BASE_URL}/{org}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            all_orgs_details[org] = response.json()
        except requests.RequestException as e:
            print(f"Error fetching details for org {org}: {e}")
        time.sleep(0.2)

    return all_orgs_details


def filter_england(all_orgs_details: Dict[str, Any]) -> Dict[str, Any]:
    filtered_org_details = {
        org: data
        for org, data in all_orgs_details.items()
        if data["Organisation"]["GeoLoc"]["Location"]["Country"] == "ENGLAND"
    }
    return filtered_org_details


def process_org_details(all_orgs_details: Dict[str, Any]) -> tuple:
    """
    Processes organisation details.
    Filters to organisations in England.

    Returns a tuple of dictionaries:
    - names: A dictionary with the ODS codes as keys and the names as values.
    - predecessors: A dictionary with the ODS codes as keys and the predecessor ODS codes as values.
    - icbs: A dictionary with the ODS codes as keys and the ICB ODS codes as values.
    """
    names = {
        org: data["Organisation"]["Name"] for org, data in all_orgs_details.items()
    }

    filtered_org_details = filter_england(all_orgs_details)

    predecessors = {}
    icbs = {}

    for current_code, org_data in filtered_org_details.items():
        org = org_data["Organisation"]
        predecessors[current_code] = [
            succ["Target"]["OrgId"]["extension"]
            for succ in org.get("Succs", {}).get("Succ", [])
            if succ["Type"] == "Predecessor"
        ]

        if "Rels" in org:
            for rel in org["Rels"]["Rel"]:
                if rel["id"] == "RE5":
                    icbs[current_code] = rel["Target"]["OrgId"]["extension"]

    return names, predecessors, icbs


def get_org_details_dict(
    org_details: Dict[str, Any], icbs: Dict[str, str]
) -> Dict[str, Any]:
    """
    From organisation details json, return a dictionary of the salient fields.
    """

    if not org_details:
        return {}

    org = org_details["Organisation"]
    org_icb = icbs.get(org["OrgId"]["extension"], None)

    dates = {
        date["Type"]: {"Start": date.get("Start"), "End": date.get("End")}
        for date in org["Date"]
    }

    return {
        "ods_name": org["Name"],
        "postcode": org["GeoLoc"]["Location"]["PostCode"],
        "legal_closed_date": dates.get("Legal", {}).get("End"),
        "operational_closed_date": dates.get("Operational", {}).get("End"),
        "legal_open_date": dates.get("Legal", {}).get("Start"),
        "operational_open_date": dates.get("Operational", {}).get("Start"),
        "icb": org_icb,
    }


def create_org_mapping_df(
    predecessors: Dict[str, List[str]],
    orgs_details: Dict[str, Any],
    names: Dict[str, str],
    icbs: Dict[str, str],
) -> pd.DataFrame:
    columns = [
        "ods_code",
        "ods_name",
        "successor_ods_code",
        "legal_closed_date",
        "operational_closed_date",
        "legal_open_date",
        "operational_open_date",
        "succession_date",
        "postcode",
        "region",
        "icb",
    ]
    org_mapping_df = pd.DataFrame(columns=columns)

    for org, preds in predecessors.items():
        if org in org_mapping_df["ods_code"].values:
            # we've already added this org via a predecessor
            continue
        org_details = orgs_details[org]
        org_details_dict = get_org_details_dict(org_details, icbs)
        org_details_dict["ods_code"] = org
        org_details_dict["successor_ods_code"] = None
        org_mapping_df = pd.concat(
            [org_mapping_df, pd.DataFrame([org_details_dict])], ignore_index=True
        )

        for pred in preds:
            if pred not in orgs_details:
                continue
            
            if pred in org_mapping_df["ods_code"].values:
                org_mapping_df.loc[org_mapping_df["ods_code"]==pred, "successor_ods_code"] = org
            else:
                pred_details = orgs_details.get(pred, {})
                pred_details_dict = get_org_details_dict(pred_details, icbs)
                pred_details_dict["ods_code"] = pred
                pred_details_dict["successor_ods_code"] = org

                org_mapping_df = pd.concat(
                    [org_mapping_df, pd.DataFrame([pred_details_dict])], ignore_index=True
                )

    return org_mapping_df


def get_icb_regions(icbs: List[str]) -> Dict[str, str]:
    """
    Get the region for each ICB in the list.
    """
    icb_regions = {}
    icb_details = get_org_details(icbs)
    for icb, details in icb_details.items():
        rels = details.get("Organisation", {}).get("Rels", {}).get("Rel", [])
        for rel in rels:
            if rel["id"] == "RE2":
                region = rel["Target"]["OrgId"]["extension"]
                icb_regions[icb] = region
    return icb_regions


def get_region_names(regions: set) -> Dict[str, str]:
    """
    Get the region names for a set of regions.
    """
    region_names = {}
    region_details = get_org_details(list(regions))
    for region, details in region_details.items():
        region_name = details.get("Organisation", {}).get("Name", "")
        if region_name:
            region_names[region] = region_name
        else:
            print(f"Error fetching region name for {region}")
    return region_names

def format_org_name(name):

    # List of acronyms to preserve
    acronyms = ["NHS"]
    drop_strings = ["COMMISSIONING REGION"]

    for drop_string in drop_strings:
        name = name.replace(drop_string, "")
    
    words = name.split()
    formatted_words = [word if word in acronyms else word.title() for word in words]
    formatted_name = ' '.join(formatted_words)
    formatted_name = formatted_name.strip()

    return formatted_name

def upload_bq(df: pd.DataFrame) -> None:

    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    client = get_bigquery_client()
    
    dtypes = {
        "ods_code": str,
        "ods_name": str,
        "successor_ods_code": str,
        "legal_closed_date": "datetime64[ns]",
        "operational_closed_date": "datetime64[ns]",
        "legal_open_date": "datetime64[ns]",
        "operational_open_date": "datetime64[ns]",
        "succession_date": "datetime64[ns]",
        "postcode": str,
        "region": str,
        "icb": str,
    }

    df = df.astype(dtypes)

    schema = [
        bigquery.SchemaField("ods_code", "STRING", description="ODS code of the organisation"),
        bigquery.SchemaField("ods_name", "STRING", description="Name of the organisation"),
        bigquery.SchemaField("successor_ods_code", "STRING", description="ODS code of the successor organisation"),
        bigquery.SchemaField("legal_closed_date", "DATE", description="Legal closed date of the organisation"),
        bigquery.SchemaField("operational_closed_date", "DATE", description="Operational closed date of the organisation"),
        bigquery.SchemaField("legal_open_date", "DATE", description="Legal open date of the organisation"),
        bigquery.SchemaField("operational_open_date", "DATE", description="Operational open date of the organisation"),
        bigquery.SchemaField("succession_date", "TIMESTAMP", description="Succession date of the organisation where it has a sucessor"),
        bigquery.SchemaField("postcode", "STRING", description="Postcode of the organisation"),
        bigquery.SchemaField("region", "STRING", description="Region of the organisation"),
        bigquery.SchemaField("icb", "STRING", description="ICB of the organisation"),
    ]



    try:
        table = client.get_table(full_table_id)
        print(f"Table {full_table_id} already exists with {table.num_rows} rows.")
    except NotFound:
    
        table = bigquery.Table(full_table_id, schema=schema)
        table.description = "NHS Trusts with mapping to successor trusts"
        table = client.create_table(table)
        print(f"Table {full_table_id} created.")
    
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = client.load_table_from_dataframe(
            df, full_table_id, job_config=job_config
        )

        job.result()

        print(f"Loaded {job.output_rows} rows into {full_table_id}")
    
