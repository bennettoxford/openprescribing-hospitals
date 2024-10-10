import os
import requests
import zipfile
import re
import pandas as pd
import xml.etree.ElementTree as ET

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

class Command(BaseCommand):
    help = "Imports dm+d supplementary data into BigQuery"

    def handle(self, *args, **options):
        df = self.fetch_dmd_supp_data()
        self.upload_bq(df)
        self.stdout.write(self.style.SUCCESS("Successfully imported dm+d supplementary data into BigQuery"))

    def fetch_dmd_supp_data(self):
        url = f"https://isd.digital.nhs.uk/trud/api/v1/keys/{TRUD_KEY}/items/25/releases?latest"

        release = requests.get(url).json()['releases'][0]

        release_date = release["releaseDate"]
        download_url = release["archiveFileUrl"]

        release_date_obj = datetime.strptime(release_date, "%Y-%m-%d")
        week_number = release_date_obj.isocalendar()[1]
        year = release_date_obj.isocalendar()[0]

        data = requests.get(download_url)

        with open("temp.zip", "wb") as temp_file:
            temp_file.write(data.content)

        with zipfile.ZipFile("temp.zip", 'r') as zip_file:
            xml_file_pattern = rf"week{week_number}{year}.*BNF\.zip"
            bnf_zip_files = [f for f in zip_file.namelist() if re.match(xml_file_pattern, f)]
            
            if not bnf_zip_files:
                raise FileNotFoundError(f"No BNF zip file found matching pattern: {xml_file_pattern}")
            
            bnf_zip_file = bnf_zip_files[0]
            
            zip_file.extract(bnf_zip_file)
            
            with zipfile.ZipFile(bnf_zip_file, 'r') as bnf_zip:
                xml_files = [f for f in bnf_zip.namelist() if f.endswith('.xml')]
                
                if not xml_files:
                    raise FileNotFoundError(f"No XML file found in {bnf_zip_file}")
                
                xml_file = xml_files[0]
            
                with bnf_zip.open(xml_file) as xml_content:
                    tree = ET.parse(xml_content)
                    root = tree.getroot()

            vmp_codes = []
            bnf_codes = []
            atc_codes = []

            for vmp in root.findall('.//VMP'):
                vmp_code = vmp.find('VPID').text
                bnf_code = vmp.find('BNF')
                atc_code = vmp.find('ATC')
                
                vmp_codes.append(vmp_code)
                bnf_codes.append(bnf_code.text if bnf_code is not None else None)
                atc_codes.append(atc_code.text if atc_code is not None else None)

            df = pd.DataFrame({
                'vmp_code': vmp_codes,
                'bnf_code': bnf_codes,
                'atc_code': atc_codes
            })

        os.remove("temp.zip")
        os.remove(bnf_zip_file)
        return df

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
