import os
import requests
import zipfile
import re
import pandas as pd
import xml.etree.ElementTree as ET

from dataclasses import dataclass
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime
from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from viewer.management.utils import get_bigquery_client, PROJECT_ID


load_dotenv()

TRUD_KEY = os.getenv("TRUD_API_KEY")
DATASET_ID = "scmd"
TABLE_ID = "scmd_dmd_supp"

@dataclass
class TRUDRelease:
    release_date: str
    download_url: str
    year: int

class TRUDClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://isd.digital.nhs.uk/trud/api/v1"

    def get_latest_release(self) -> TRUDRelease:
        url = f"{self.base_url}/keys/{self.api_key}/items/25/releases?latest"
        response = requests.get(url)
        response.raise_for_status()
        
        release = response.json()['releases'][0]
        release_date = release["releaseDate"]
        release_date_obj = datetime.strptime(release_date, "%Y-%m-%d")
        
        return TRUDRelease(
            release_date=release_date,
            download_url=release["archiveFileUrl"],
            year=release_date_obj.isocalendar()[0]
        )

class Command(BaseCommand):
    help = "Imports dm+d supplementary data into BigQuery"

    def __init__(self):
        super().__init__()
        self.trud_client = TRUDClient(TRUD_KEY)
        self.temp_dir = Path("temp")

    def handle(self, *args, **options):
        try:
            self.temp_dir.mkdir(exist_ok=True)
            df = self.fetch_dmd_supp_data()
            self.upload_bq(df)
            self.stdout.write(self.style.SUCCESS("Successfully imported dm+d supplementary data"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error: {str(e)}"))
            raise
        finally:
            self._cleanup_temp_files()

    def _cleanup_temp_files(self) -> None:
        if self.temp_dir.exists():
            for file in self.temp_dir.glob("*"):
                file.unlink()
            self.temp_dir.rmdir()

    def _extract_xml_data(self, xml_content) -> pd.DataFrame:
        tree = ET.parse(xml_content)
        root = tree.getroot()

        data = []
        for vmp in root.findall('.//VMP'):
            vmp_code = vmp.find('VPID').text
            bnf_code = vmp.find('BNF')
            atc_code = vmp.find('ATC')
            
            data.append({
                'vmp_code': vmp_code,
                'bnf_code': bnf_code.text if bnf_code is not None else None,
                'atc_code': atc_code.text if atc_code is not None else None
            })

        return pd.DataFrame(data)

    def fetch_dmd_supp_data(self) -> pd.DataFrame:
        release = self.trud_client.get_latest_release()
        
        response = requests.get(release.download_url)
        response.raise_for_status()
        
        main_zip_path = self.temp_dir / "dmd_release.zip"
        with open(main_zip_path, "wb") as temp_file:
            temp_file.write(response.content)

        bnf_zip_pattern = rf"week\d\d{release.year}.*BNF\.zip"
        
        with zipfile.ZipFile(main_zip_path, 'r') as zip_file:
            bnf_zip_files = [f for f in zip_file.namelist() if re.match(bnf_zip_pattern, f)]
            
            if not bnf_zip_files:
                raise FileNotFoundError(f"No BNF zip file found matching pattern: {bnf_zip_pattern}")
            
            bnf_zip_path = self.temp_dir / bnf_zip_files[0]
            zip_file.extract(bnf_zip_files[0], self.temp_dir)
            
            with zipfile.ZipFile(bnf_zip_path, 'r') as bnf_zip:
                xml_files = [f for f in bnf_zip.namelist() if f.endswith('.xml')]
                
                if not xml_files:
                    raise FileNotFoundError(f"No XML file found in {bnf_zip_path}")
                
                with bnf_zip.open(xml_files[0]) as xml_content:
                    return self._extract_xml_data(xml_content)

    def upload_bq(self, df: pd.DataFrame) -> None:
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        client = get_bigquery_client()

        dtypes = {
            "vmp_code": str,
            "bnf_code": str,
            "atc_code": str,
        }

        df = df.astype(dtypes)

        schema = [
            bigquery.SchemaField(
                "vmp_code", "STRING", description="VMP code of the drug"
            ),
            bigquery.SchemaField(
                "bnf_code", "STRING", description="BNF code of the drug"
            ),
            bigquery.SchemaField(
                "atc_code", "STRING", description="ATC code of the drug"
            ),
        ]

        try:
            table = client.get_table(full_table_id)
            self.stdout.write(f"Table {full_table_id} already exists with {table.num_rows} rows.")
        except NotFound:
            table = bigquery.Table(full_table_id, schema=schema)
            table.description = "dm+d supplementary data mapping vmp to bnf and atc"
            table = client.create_table(table)
            self.stdout.write(f"Table {full_table_id} created.")

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)

        job.result()

        self.stdout.write(f"Loaded {job.output_rows} rows into {full_table_id}")
