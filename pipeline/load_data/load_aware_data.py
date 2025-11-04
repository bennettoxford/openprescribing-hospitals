import pandas as pd

from prefect import get_run_logger, task, flow
from django.db import transaction
from typing import Dict
from pipeline.utils.utils import setup_django_environment, fetch_table_data_from_bq
from pipeline.setup.bq_tables import AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC

setup_django_environment()
from viewer.models import AWAREAntibiotic, AWAREVMPMapping, VMP, VTM


@task()
def extract_aware_data() -> pd.DataFrame:
    """Extract processed AWaRe data from BigQuery table"""
    logger = get_run_logger()
    logger.info("Starting extraction of processed AWaRe data from BigQuery")

    df = fetch_table_data_from_bq(AWARE_VMP_MAPPING_PROCESSED_TABLE_SPEC, use_bqstorage=False)

    logger.info(f"Extracted {len(df)} rows of AWaRe data")
    return df


@task()
def load_aware_antibiotics(aware_data: pd.DataFrame) -> Dict[str, int]:
    """Replace all AWAREAntibiotic records with new data"""
    logger = get_run_logger()

    unique_antibiotics = aware_data.groupby('Antibiotic').agg({
        'aware_2019': 'first',
        'aware_2024': 'first'
    }).reset_index()

    logger.info(f"Found {len(unique_antibiotics)} unique antibiotics in the data")

    with transaction.atomic():
        logger.info("Deleting AWAREAntibiotic records...")
        total_deleted = AWAREAntibiotic.objects.all().delete()[0]
        logger.info(f"Finished deleting AWAREAntibiotic records. Total deleted: {total_deleted}")

        antibiotic_objects = []
        for _, row in unique_antibiotics.iterrows():
            antibiotic_obj = AWAREAntibiotic(
                name=row['Antibiotic'],
                aware_2019=row.get('aware_2019'),
                aware_2024=row.get('aware_2024')
            )
            antibiotic_objects.append(antibiotic_obj)

        created_objects = AWAREAntibiotic.objects.bulk_create(antibiotic_objects, batch_size=1000)
        logger.info(f"Created {len(created_objects)} AWAREAntibiotic records")
        
        antibiotic_mapping = {antibiotic.name: antibiotic.id for antibiotic in AWAREAntibiotic.objects.all()}

    return antibiotic_mapping


@task()
def validate_existing_vmps_vtms(aware_data: pd.DataFrame) -> tuple[Dict[str, int], Dict[str, int]]:
    """Validate that VMPs and VTMs referenced in AWaRe data exist in the database"""
    logger = get_run_logger()

    vmp_codes = set(aware_data['vmp_id'].astype(str).unique())
    vtm_codes = set(aware_data['vtm_id'].astype(str).unique())

    logger.info(f"Found {len(vmp_codes)} unique VMP codes in AWaRe data")
    logger.info(f"Found {len(vtm_codes)} unique VTM codes in AWaRe data")

    existing_vmps = VMP.objects.filter(code__in=vmp_codes)
    existing_vmp_codes = {vmp.code for vmp in existing_vmps}
    missing_vmp_codes = vmp_codes - existing_vmp_codes

    if missing_vmp_codes:
        logger.warning(f"Missing VMP codes that will be skipped: {missing_vmp_codes}")

    vmp_mapping = {vmp.code: vmp.id for vmp in existing_vmps}

    existing_vtms = VTM.objects.filter(vtm__in=vtm_codes)
    existing_vtm_codes = {vtm.vtm for vtm in existing_vtms}
    missing_vtm_codes = vtm_codes - existing_vtm_codes

    if missing_vtm_codes:
        logger.warning(f"Missing VTM codes that will be skipped: {missing_vtm_codes}")

    vtm_mapping = {vtm.vtm: vtm.id for vtm in existing_vtms}

    logger.info(f"Found {len(existing_vmp_codes)} existing VMPs out of {len(vmp_codes)} required")
    logger.info(f"Found {len(existing_vtm_codes)} existing VTMs out of {len(vtm_codes)} required")

    return vmp_mapping, vtm_mapping


@task()
def load_aware_vmp_mappings(
    aware_data: pd.DataFrame,
    antibiotic_mapping: Dict[str, int],
    vmp_mapping: Dict[str, int],
    vtm_mapping: Dict[str, int]
) -> None:
    """Replace all AWAREVMPMapping records with new data"""
    logger = get_run_logger()

    with transaction.atomic():
        logger.info("Deleting AWAREVMPMapping records...")
        total_deleted = AWAREVMPMapping.objects.all().delete()[0]
        logger.info(f"Finished deleting AWAREVMPMapping records. Total deleted: {total_deleted}")

        mapping_objects = []
        skipped_count = 0

        for _, row in aware_data.iterrows():
            antibiotic_name = row['Antibiotic']
            vmp_code = str(row['vmp_id'])
            vtm_code = str(row['vtm_id'])

            antibiotic_id = antibiotic_mapping.get(antibiotic_name)
            vmp_id = vmp_mapping.get(vmp_code)
            vtm_id = vtm_mapping.get(vtm_code)

            if antibiotic_id and vmp_id and vtm_id:
                mapping_obj = AWAREVMPMapping(
                    aware_antibiotic_id=antibiotic_id,
                    vmp_id=vmp_id,
                    vtm_id=vtm_id
                )
                mapping_objects.append(mapping_obj)
            else:
                skipped_count += 1

        if skipped_count > 0:
            logger.warning(f"Skipped {skipped_count} mappings due to missing references")

        seen_combinations = set()
        unique_mapping_objects = []
        for obj in mapping_objects:
            key = (obj.aware_antibiotic_id, obj.vmp_id)
            if key not in seen_combinations:
                seen_combinations.add(key)
                unique_mapping_objects.append(obj)

        duplicate_count = len(mapping_objects) - len(unique_mapping_objects)
        if duplicate_count > 0:
            logger.info(f"Removed {duplicate_count} duplicate mappings")

        created_objects = AWAREVMPMapping.objects.bulk_create(unique_mapping_objects, batch_size=1000)
        logger.info(f"Created {len(created_objects)} AWAREVMPMapping records")




@flow(name="Load AWaRe Data")
def load_aware_data():
    logger = get_run_logger()
    logger.info("Starting AWaRe data load")

    try:
        aware_data = extract_aware_data()
        antibiotic_mapping = load_aware_antibiotics(aware_data)
        vmp_mapping, vtm_mapping = validate_existing_vmps_vtms(aware_data)
        load_aware_vmp_mappings(aware_data, antibiotic_mapping, vmp_mapping, vtm_mapping)
        
        logger.info("AWaRe data load completed successfully")


    except Exception as e:
        logger.error(f"Error in AWaRe data load flow: {str(e)}")



if __name__ == "__main__":
    load_aware_data()