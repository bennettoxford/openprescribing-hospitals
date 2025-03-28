import requests
import pandas as pd
import time
import logging

from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from tqdm import tqdm
from typing import Dict, List, Any
from django.core.management.base import BaseCommand

from viewer.management.utils import get_bigquery_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent
PROJECT_ID = "ebmdatalab"
DATASET_ID = "scmd"
TABLE_ID = "ods_mapped"
BASE_URL = "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations"
ROLES = ["RO197", "RO24"]  # NHS TRUST, NHS TRUST DERIVED


class ORDAPIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url
    
    def get_all_orgs(self, roles: List[str]) -> List[str]:  # Added self parameter
        """
        Get a list of ODS codes for all organisations with the specified roles.

        Args:
            roles (List[str]): The roles to filter by.

        Returns:
            List[str]: A list of ODS codes for all organisations with the specified roles.
        """
        urls = [f"{self.base_url}?Roles={role}&Limit=1000" for role in roles]  # Use self.base_url
        all_orgs = []
        for url in urls:
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()["Organisations"]
                all_orgs.extend([org["OrgId"] for org in data])
            except requests.RequestException as e:
                logger.error(f"Error fetching organizations: {e}")
        return all_orgs

    def get_org_details(self, orgs: List[str]) -> Dict[str, Any]:  # Added self parameter
        """
        Get details for a list of organisations.

        Args:
            orgs (List[str]): The ODS codes of the organisations to get details for.

        Returns:
            Dict[str, Any]: A dictionary with the ODS codes as keys and the details as values.
        """
        all_orgs_details = {}
        for org in tqdm(orgs):
            url = f"{self.base_url}/{org}"  # Use self.base_url
            try:
                response = requests.get(url)
                response.raise_for_status()
                all_orgs_details[org] = response.json()
            except requests.RequestException as e:
                logger.error(f"Error fetching details for org {org}: {e}")
            time.sleep(0.2)

        return all_orgs_details


class OrganisationDataProcessor:
    """Handles processing of organisation details"""

    @staticmethod
    def _filter_england(all_orgs_details: Dict[str, Any]) -> Dict[str, Any]:
        filtered_org_details = {
            org: data
            for org, data in all_orgs_details.items()
            if data["Organisation"]["GeoLoc"]["Location"]["Country"] == "ENGLAND"
        }
        return filtered_org_details

    @staticmethod
    def process_org_details(all_orgs_details: Dict[str, Any]) -> tuple:
        """
        Processes organisation details.
        Filters to organisations in England.

        Returns a tuple of dictionaries:
        - names: A dictionary with the ODS codes as keys and the names as values.
        - predecessors: A dictionary with the ODS codes as keys and the predecessor ODS codes as values.
        - icbs: A dictionary with the ODS codes as keys and the ICB ODS codes as values.
        """
    
        filtered_org_details = OrganisationDataProcessor._filter_england(all_orgs_details)

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

        return predecessors, icbs


class DataFrameBuilder:
    def __init__(self):
        self.columns = [
            "ods_code", "ods_name", "successor_ods_code",
            "legal_closed_date", "operational_closed_date",
            "legal_open_date", "operational_open_date",
            "succession_date", "postcode", "region", "icb",
        ]

    @staticmethod
    def _get_org_details_dict(
        org_details: Dict[str, Any], icbs: Dict[str, str]) -> Dict[str, Any]:
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
        self,
        predecessors: Dict[str, List[str]],
        orgs_details: Dict[str, Any],
        icbs: Dict[str, str]) -> pd.DataFrame:

            columns = self.columns
            org_mapping_df = pd.DataFrame(columns=columns)

            for org, preds in predecessors.items():
                if org in org_mapping_df["ods_code"].values:
                    # we've already added this org via a predecessor
                    continue
                org_details = orgs_details[org]
                org_details_dict = self._get_org_details_dict(org_details, icbs)
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
                        pred_details_dict = self._get_org_details_dict(pred_details, icbs)
                        pred_details_dict["ods_code"] = pred
                        pred_details_dict["successor_ods_code"] = org

                        org_mapping_df = pd.concat(
                            [org_mapping_df, pd.DataFrame([pred_details_dict])], ignore_index=True
                        )

            return org_mapping_df

class RegionDataProcessor:
    """Handles processing of region and ICB data"""
    
    def __init__(self, api_client: ORDAPIClient):
        self.api_client = api_client

    def get_icb_regions(self, icbs: List[str]) -> Dict[str, str]:
        """Get the region for each ICB in the list."""
        icb_regions = {}
        icb_details = self.api_client.get_org_details(icbs)
        
        for icb, details in icb_details.items():
            rels = details.get("Organisation", {}).get("Rels", {}).get("Rel", [])
            for rel in rels:
                if rel["id"] == "RE2":
                    region = rel["Target"]["OrgId"]["extension"]
                    icb_regions[icb] = region
        return icb_regions

    def get_region_or_icb_names(self, org_codes: set) -> Dict[str, str]:
        """
        Get organisation names for a set of organisation codes (regions or ICBs).

        Args:
            org_codes (set): Set of organisation codes to fetch names for

        Returns:
            Dict[str, str]: Dictionary mapping organisation codes to their names
        """
        org_names = {}
        org_details = self.api_client.get_org_details(list(org_codes))
        
        for org_code, details in org_details.items():
            org_name = details.get("Organisation", {}).get("Name", "")
            if org_name:
                org_names[org_code] = org_name
            else:
                logger.error(f"Error fetching name for organisation {org_code}")
        return org_names

    @staticmethod
    def format_org_name(name: str) -> str:
        """Format organisation name with proper capitalisation"""
        if not name:
            return name
        
        acronyms = {"NHS"}
        drop_strings = {"COMMISSIONING REGION", "INTEGRATED CARE BOARD"}

        for drop_string in drop_strings:
            name = name.replace(drop_string, "")
        
        words = name.split()
        formatted_words = [word if word in acronyms else word.title() for word in words]
        formatted_name = ' '.join(formatted_words).strip()
        
        # Fix apostrophe capitalisation after title casing
        formatted_name = formatted_name.replace("'S ", "'s ").replace("'S,", "'s,")
        
        return formatted_name



class BigQueryUploader:
    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        self.full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        self.client = get_bigquery_client()
        self.schema = self._get_schema()

    def _get_schema(self) -> List[bigquery.SchemaField]:
        return [
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

    def _prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
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
        return df.astype(dtypes)

    def upload(self, df: pd.DataFrame) -> None:
        df = self._prepare_df(df)
        full_table_id = self.full_table_id
        schema = self.schema

        try:
            table = self.client.get_table(full_table_id)
            logger.info(f"Table {full_table_id} already exists with {table.num_rows} rows.")
        except NotFound:
        
            table = bigquery.Table(full_table_id, schema=schema)
            table.description = "NHS Trusts with mapping to successor trusts"
            table = self.client.create_table(table)
            logger.info(f"Table {full_table_id} created.")
        
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )

            job = self.client.load_table_from_dataframe(
                df, full_table_id, job_config=job_config
            )

            job.result()

            logger.info(f"Loaded {job.output_rows} rows into {full_table_id}")


class OrganisationImporter:
    def __init__(self, roles: List[str]):
        self.roles = roles
        self.api_client = ORDAPIClient(BASE_URL)
        self.data_processor = OrganisationDataProcessor()
        self.data_frame_builder = DataFrameBuilder()
        self.bigquery_uploader = BigQueryUploader(PROJECT_ID, DATASET_ID, TABLE_ID)
        self.region_data_processor = RegionDataProcessor(self.api_client)
        
    def import_organisations(self) -> None:
        all_orgs = self.api_client.get_all_orgs(self.roles)
        all_orgs_details = self.api_client.get_org_details(all_orgs)
        predecessors, icbs = self.data_processor.process_org_details(all_orgs_details)
        org_mapping_df = self.data_frame_builder.create_org_mapping_df(predecessors, all_orgs_details, icbs)

        icbs_list = org_mapping_df["icb"].dropna().unique().tolist()
        icb_regions = self.region_data_processor.get_icb_regions(icbs_list)
        unique_regions = set(icb_regions.values())
        region_names = self.region_data_processor.get_region_or_icb_names(unique_regions)

        icb_names = self.region_data_processor.get_region_or_icb_names(icbs_list)

        org_mapping_df["region"] = org_mapping_df["icb"].map(icb_regions).map(region_names)
        org_mapping_df["icb"] = org_mapping_df["icb"].map(icb_names)
        org_mapping_df.loc[org_mapping_df["ods_name"].notna(), "ods_name"] = org_mapping_df.loc[org_mapping_df["ods_name"].notna(), "ods_name"].apply(self.region_data_processor.format_org_name)
        org_mapping_df.loc[org_mapping_df["region"].notna(), "region"] = org_mapping_df.loc[org_mapping_df["region"].notna(), "region"].apply(self.region_data_processor.format_org_name)
        org_mapping_df.loc[org_mapping_df["icb"].notna(), "icb"] = org_mapping_df.loc[org_mapping_df["icb"].notna(), "icb"].apply(self.region_data_processor.format_org_name)
 
        self.bigquery_uploader.upload(org_mapping_df)


class Command(BaseCommand):
    help = "Imports organisation data into BigQuery"

    def handle(self, *args, **options):
        importer = OrganisationImporter(ROLES)
        importer.import_organisations()
        self.stdout.write(self.style.SUCCESS("Successfully imported organisation data into BigQuery"))
