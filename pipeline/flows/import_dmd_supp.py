import requests
import zipfile
import re
import xml.etree.ElementTree as ET

from dataclasses import dataclass
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime
from prefect import task, flow
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret

from pipeline.utils.utils import get_bigquery_client
from pipeline.bq_tables import DMD_SUPP_TABLE_SPEC, VTM_INGREDIENTS_TABLE_SPEC, DMD_HISTORY_TABLE_SPEC


TRUD_KEY = Secret.load("trud-api-key").get()
TEMP_DIR = Path("temp")


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

        release = response.json()["releases"][0]
        release_date = release["releaseDate"]
        release_date_obj = datetime.strptime(release_date, "%Y-%m-%d")

        return TRUDRelease(
            release_date=release_date,
            download_url=release["archiveFileUrl"],
            year=release_date_obj.isocalendar()[0],
        )


@task
def create_temp_directory():
    """Create temporary directory for downloads"""
    logger = get_run_logger()
    logger.info("Creating temporary directory")
    TEMP_DIR.mkdir(exist_ok=True)


@task
def cleanup_temp_files():
    """Clean up temporary files"""
    logger = get_run_logger()
    logger.info("Cleaning up temporary files")
    
    if TEMP_DIR.exists():
        for file in TEMP_DIR.rglob("*"):
            try:
                if file.is_file():
                    file.unlink(missing_ok=True)
            except (PermissionError, OSError) as e:
                logger.warning(f"Could not remove file {file}: {str(e)}")

        dirs_to_remove = [d for d in TEMP_DIR.rglob("*") if d.is_dir()]
        for dir_path in reversed(dirs_to_remove):
            try:
                dir_path.rmdir()
            except (PermissionError, OSError) as e:
                logger.warning(f"Could not remove directory {dir_path}: {str(e)}")

        try:
            TEMP_DIR.rmdir()
        except (PermissionError, OSError) as e:
            logger.warning(f"Could not remove temp directory {TEMP_DIR}: {str(e)}")


@task
def get_latest_trud_release() -> TRUDRelease:
    """Get the latest TRUD release information"""
    logger = get_run_logger()
    logger.info("Fetching latest TRUD release information")
    trud_client = TRUDClient(TRUD_KEY)
    return trud_client.get_latest_release()


@task
def download_dmd_release(release: TRUDRelease) -> Path:
    """Download the latest dm+d release zip file"""
    logger = get_run_logger()
    logger.info(f"Downloading dm+d release from {release.release_date}")

    response = requests.get(release.download_url)
    response.raise_for_status()

    main_zip_path = TEMP_DIR / "dmd_release.zip"
    with open(main_zip_path, "wb") as temp_file:
        temp_file.write(response.content)

    return main_zip_path


@task
def extract_supplementary_files(main_zip_path: Path, release_year: int) -> tuple[Path, Path, Path]:
    """Extract BNF XML file and find VTM ingredients and history files"""
    logger = get_run_logger()
    logger.info("Extracting supplementary files")

    bnf_zip_pattern = rf"week\d\d{release_year}.*BNF\.zip"
    vtm_ing_pattern = rf"VTM_INGREDIENTS/f_vtm_ing1_\d+\.xml"
    history_pattern = rf"HISTORIC_CODES/f_history1_\d+\.xml"

    with zipfile.ZipFile(main_zip_path, "r") as zip_file:
        bnf_zip_files = [f for f in zip_file.namelist() if re.match(bnf_zip_pattern, f)]
        if not bnf_zip_files:
            raise FileNotFoundError(f"No BNF zip file found matching pattern: {bnf_zip_pattern}")

        bnf_zip_path = TEMP_DIR / bnf_zip_files[0]
        zip_file.extract(bnf_zip_files[0], TEMP_DIR)

        # Extract VTM ingredients file
        vtm_ing_files = [f for f in zip_file.namelist() if re.match(vtm_ing_pattern, f)]
        if not vtm_ing_files:
            raise FileNotFoundError(f"No VTM ingredients file found matching pattern: {vtm_ing_pattern}")
        
        vtm_ing_path = TEMP_DIR / vtm_ing_files[0]
        zip_file.extract(vtm_ing_files[0], TEMP_DIR)

        # Extract history file
        history_files = [f for f in zip_file.namelist() if re.match(history_pattern, f)]
        if not history_files:
            raise FileNotFoundError(f"No history file found matching pattern: {history_pattern}")
        
        history_path = TEMP_DIR / history_files[0]
        zip_file.extract(history_files[0], TEMP_DIR)

        with zipfile.ZipFile(bnf_zip_path, "r") as bnf_zip:
            xml_files = [f for f in bnf_zip.namelist() if f.endswith(".xml")]
            if not xml_files:
                raise FileNotFoundError(f"No XML file found in {bnf_zip_path}")
            
            bnf_xml_path = TEMP_DIR / xml_files[0]
            bnf_zip.extract(xml_files[0], TEMP_DIR)

        return bnf_xml_path, vtm_ing_path, history_path


@task
def parse_xml_data(xml_path: Path) -> list[dict]:
    """Parse the XML file to extract VMP, BNF, and ATC codes"""
    logger = get_run_logger()
    logger.info(f"Parsing XML data from {xml_path}")

    tree = ET.parse(xml_path)
    root = tree.getroot()

    data = []
    for vmp in root.findall(".//VMP"):
        vmp_code = vmp.find("VPID").text
        bnf_code = vmp.find("BNF")
        atc_code = vmp.find("ATC")
        ddd = vmp.find("DDD")
        ddd_uom = vmp.find("DDD_UOMCD")

        ddd_value = ddd.text if ddd is not None else None
        if ddd_value is not None:
            try:
                ddd_value = float(ddd_value)
            except ValueError:
                logger.warning(
                    f"Invalid DDD value for VMP {vmp_code}: {ddd_value}. Setting to None."
                )
                ddd_value = None

        data.append(
            {
                "vmp_code": str(vmp_code) if vmp_code is not None else None,
                "bnf_code": str(bnf_code.text)
                if bnf_code is not None and bnf_code.text is not None
                else None,
                "atc_code": str(atc_code.text)
                if atc_code is not None and atc_code.text is not None
                else None,
                "ddd": ddd_value,
                "ddd_uom": str(ddd_uom.text)
                if ddd_uom is not None and ddd_uom.text is not None
                else None,
            }
        )

    logger.info(f"Extracted {len(data)} records from XML")
    return data


@task
def parse_vtm_ingredients_xml(xml_path: Path) -> list[dict]:
    """Parse the VTM ingredients XML file"""
    logger = get_run_logger()
    logger.info(f"Parsing VTM ingredients XML data from {xml_path}")

    tree = ET.parse(xml_path)
    root = tree.getroot()

    data = []
    for vtm_ing in root.findall(".//VTM_ING"):
        vtm_id = vtm_ing.find("VTMID").text
        ingredient_id = vtm_ing.find("ISID").text
        
        data.append({
            "vtm_id": str(vtm_id) if vtm_id is not None else None,
            "ingredient_id": str(ingredient_id) if ingredient_id is not None else None,
        })

    logger.info(f"Extracted {len(data)} VTM ingredient mappings")
    return data


@task
def parse_dmd_history_xml(xml_path: Path) -> list[dict]:
    """Parse the dm+d history XML file for all entity types"""
    logger = get_run_logger()
    logger.info(f"Parsing dm+d history XML data from {xml_path}")

    tree = ET.parse(xml_path)
    root = tree.getroot()

    data = []

    def process_entities(entities, entity_type):
        results = []
        for entity in entities:
            current_id = entity.find("IDCURRENT").text
            previous_id = entity.find("IDPREVIOUS").text
            start_date = entity.find("STARTDT").text
            end_date = entity.find("ENDDT").text if entity.find("ENDDT") is not None else None
            
            results.append({
                "current_id": str(current_id) if current_id is not None else None,
                "previous_id": str(previous_id) if previous_id is not None else None,
                "start_date": start_date,
                "end_date": end_date,
                "entity_type": entity_type
            })
        return results

    entity_mappings = [
        (".//VTM", "VTM"),
        (".//VMP", "VMP"),
        (".//ING", "ING"),
        (".//SUPP", "SUPP"),
        (".//FORM", "FORM"),
        (".//ROUTE", "ROUTE"),
        (".//UOM", "UOM"),
    ]

    for xpath, entity_type in entity_mappings:
        entities = root.findall(xpath)
        entity_data = process_entities(entities, entity_type)
        data.extend(entity_data)
        logger.info(f"Extracted {len(entity_data)} {entity_type} history records")

    logger.info(f"Extracted {len(data)} total dm+d history records")
    return data


@task
def upload_to_bigquery(data: list[dict]):
    """Upload the extracted data to BigQuery"""
    logger = get_run_logger()
    logger.info(f"Uploading {len(data)} records to {DMD_SUPP_TABLE_SPEC.full_table_id}")

    client = get_bigquery_client()

    try:
        table = client.get_table(DMD_SUPP_TABLE_SPEC.full_table_id)
        logger.info(
            f"Table {DMD_SUPP_TABLE_SPEC.full_table_id} already exists with {table.num_rows} rows"
        )
    except NotFound:
        raise Exception(
            f"Table {DMD_SUPP_TABLE_SPEC.full_table_id} does not exist. Please run setup_bq_tables.py first"
        )

    job_config = bigquery.LoadJobConfig(
        schema=DMD_SUPP_TABLE_SPEC.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_json(
        data, DMD_SUPP_TABLE_SPEC.full_table_id, job_config=job_config
    )
    job.result()

    logger.info(
        f"Loaded {job.output_rows} rows into {DMD_SUPP_TABLE_SPEC.full_table_id}"
    )
    return job.output_rows


@task
def upload_vtm_ingredients_to_bigquery(data: list[dict]):
    """Upload VTM ingredients data to BigQuery"""
    logger = get_run_logger()
    logger.info(f"Uploading {len(data)} VTM ingredient records")

    client = get_bigquery_client()
    table_id = VTM_INGREDIENTS_TABLE_SPEC.full_table_id

    job_config = bigquery.LoadJobConfig(
        schema=VTM_INGREDIENTS_TABLE_SPEC.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()

    logger.info(f"Loaded {job.output_rows} rows into {table_id}")
    return job.output_rows


@task
def upload_dmd_history_to_bigquery(data: list[dict]):
    """Upload dm+d history data to BigQuery"""
    logger = get_run_logger()
    logger.info(f"Uploading {len(data)} dm+d history records")

    client = get_bigquery_client()
    table_id = DMD_HISTORY_TABLE_SPEC.full_table_id

    job_config = bigquery.LoadJobConfig(
        schema=DMD_HISTORY_TABLE_SPEC.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()

    logger.info(f"Loaded {job.output_rows} rows into {table_id}")
    return job.output_rows


@flow(name="Import dm+d Supplementary Data")
def import_dmd_supp_flow():
    """Main flow to import dm+d supplementary data into BigQuery"""
    logger = get_run_logger()
    logger.info("Starting dm+d supplementary data import flow")

    try:
        create_temp_directory()

        release = get_latest_trud_release()
        logger.info(f"Processing release from {release.release_date}")

        main_zip_path = download_dmd_release(release)
        bnf_xml_path, vtm_ing_path, history_path = extract_supplementary_files(main_zip_path, release.year)
        
        bnf_data = parse_xml_data(bnf_xml_path)
        vtm_ing_data = parse_vtm_ingredients_xml(vtm_ing_path)
        dmd_history_data = parse_dmd_history_xml(history_path)

        bnf_rows = 0
        if bnf_data and len(bnf_data) > 0:
            bnf_rows = upload_to_bigquery(bnf_data)
        else:
            logger.warning("No BNF data found to upload")

        vtm_ing_rows = 0
        if vtm_ing_data and len(vtm_ing_data) > 0:
            vtm_ing_rows = upload_vtm_ingredients_to_bigquery(vtm_ing_data)
        else:
            logger.warning("No VTM ingredient data found to upload")

        dmd_history_rows = 0
        if dmd_history_data and len(dmd_history_data) > 0:
            dmd_history_rows = upload_dmd_history_to_bigquery(dmd_history_data)
        else:
            logger.warning("No dm+d history data found to upload")

        logger.info(
            f"Successfully imported supplementary data: {bnf_rows} BNF rows, "
            f"{vtm_ing_rows} VTM ingredient rows, {dmd_history_rows} dm+d history rows"
        )

        return {
            "release_date": release.release_date,
            "bnf_rows": bnf_rows,
            "vtm_ing_rows": vtm_ing_rows,
            "dmd_history_rows": dmd_history_rows,
        }

    except Exception as e:
        logger.error(f"Error in dm+d supplementary data import flow: {str(e)}")
        raise

    finally:
        cleanup_temp_files()


if __name__ == "__main__":
    import_dmd_supp_flow()
