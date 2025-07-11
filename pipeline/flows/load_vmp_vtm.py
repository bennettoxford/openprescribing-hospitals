import pandas as pd
import numpy as np

from prefect import get_run_logger, task, flow
from django.db import transaction, connection
from typing import Dict
from pipeline.utils.utils import setup_django_environment, fetch_table_data_from_bq
from pipeline.bq_tables import (
    VMP_TABLE_SPEC,
    WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC,
    ADM_ROUTE_MAPPING_TABLE_SPEC,
)

setup_django_environment()
from viewer.models import VMP, VTM, Ingredient, WHORoute, ATC, OntFormRoute, VMPIngredientStrength


@task()
def extract_vmp_data() -> pd.DataFrame:
    """Extract VMP data from BigQuery table"""
    logger = get_run_logger()
    logger.info("Starting extraction of VMP data from BigQuery")

    df = fetch_table_data_from_bq(VMP_TABLE_SPEC, use_bqstorage=False)

    logger.info(f"Extracted {len(df)} rows of VMP data")
    return df


@task()
def extract_who_routes() -> pd.DataFrame:
    """Extract WHO routes from BigQuery table"""
    logger = get_run_logger()
    logger.info("Starting extraction of WHO routes from BigQuery")

    df = fetch_table_data_from_bq(
        WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC, use_bqstorage=False
    )

    logger.info(f"Extracted {len(df)} rows of WHO routes data")
    return df


@task()
def extract_route_mapping() -> pd.DataFrame:
    """Extract route mapping from BigQuery table"""
    logger = get_run_logger()
    logger.info("Starting extraction of route mapping from BigQuery")

    df = fetch_table_data_from_bq(ADM_ROUTE_MAPPING_TABLE_SPEC, use_bqstorage=False)

    logger.info(f"Extracted {len(df)} rows of route mapping data")
    return df


@task()
def load_vtms(vmp_data: pd.DataFrame) -> Dict[str, int]:
    """Replace all VTMs with new data"""
    logger = get_run_logger()

    vtm_entries = {}
    for _, row in vmp_data.iterrows():
        if pd.notna(row.get("vtm_code")) and pd.notna(row.get("vtm_name")):
            vtm_entries[row["vtm_code"]] = row["vtm_name"]

    logger.info(f"Found {len(vtm_entries)} unique VTMs in the data")

    with transaction.atomic():
        logger.info("Deleting VTM records...")
        deleted_total = 0
        while VTM.objects.exists():
            ids = VTM.objects.values_list('id', flat=True)[:10000]
            batch_count = VTM.objects.filter(id__in=ids).delete()[0]
            deleted_total += batch_count
        logger.info(f"Finished deleting VTM records. Total deleted: {deleted_total}")

        vtm_objects = [
            VTM(vtm=vtm_code, name=vtm_name)
            for vtm_code, vtm_name in vtm_entries.items()
        ]

        created_objects = VTM.objects.bulk_create(vtm_objects, batch_size=1000)
        logger.info(f"Created {len(created_objects)} VTM records")

        vtm_mapping = {vtm.vtm: vtm.id for vtm in VTM.objects.all()}

    return vtm_mapping


@task()
def load_who_routes(who_routes_data: pd.DataFrame) -> Dict[str, int]:
    """Replace all WHO routes with new data"""
    logger = get_run_logger()

    with transaction.atomic():
        logger.info("Deleting WHO route records...")
        deleted_total = 0
        while WHORoute.objects.exists():
            ids = WHORoute.objects.values_list('id', flat=True)[:10000]
            batch_count = WHORoute.objects.filter(id__in=ids).delete()[0]
            deleted_total += batch_count
        logger.info(f"Finished deleting WHO route records. Total deleted: {deleted_total}")

        route_objects = [
            WHORoute(code=row["who_route_code"], name=row["who_route_description"])
            for _, row in who_routes_data.iterrows()
        ]

        created_objects = WHORoute.objects.bulk_create(route_objects, batch_size=1000)
        logger.info(f"Created {len(created_objects)} WHO route records")

        route_mapping = {route.code: route.id for route in WHORoute.objects.all()}

    return route_mapping


@task()
def load_ingredients(vmp_data: pd.DataFrame) -> Dict[str, int]:
    """Replace all ingredients with new data"""
    logger = get_run_logger()

    ingredient_entries = {}
    for _, row in vmp_data.iterrows():
        if "ingredients" in row and (
            isinstance(row["ingredients"], list)
            or isinstance(row["ingredients"], np.ndarray)
        ):
            for ing in row["ingredients"]:
                ing_code = ing.get("ingredient_code")
                ing_name = ing.get("ingredient_name")

                if isinstance(ing, dict) and ing_code and ing_name:
                    ingredient_entries[ing_code] = ing_name

    logger.info(f"Found {len(ingredient_entries)} unique ingredients in the data")

    with transaction.atomic():
        logger.info("Deleting ingredient records...")
        deleted_total = 0
        while Ingredient.objects.exists():
            ids = Ingredient.objects.values_list('id', flat=True)[:10000]
            batch_count = Ingredient.objects.filter(id__in=ids).delete()[0]
            deleted_total += batch_count
        logger.info(f"Finished deleting ingredient records. Total deleted: {deleted_total}")

        ingredient_objects = [
            Ingredient(code=ing_code, name=ing_name)
            for ing_code, ing_name in ingredient_entries.items()
        ]

        created_objects = Ingredient.objects.bulk_create(
            ingredient_objects, batch_size=1000
        )
        logger.info(f"Created {len(created_objects)} ingredient records")

        ingredient_mapping = {ing.code: ing.id for ing in Ingredient.objects.all()}

    return ingredient_mapping


@task()
def validate_atcs(vmp_data: pd.DataFrame) -> Dict[str, int]:
    """Get mapping of existing ATC codes"""
    logger = get_run_logger()

    atc_codes = set()
    for _, row in vmp_data.iterrows():
        if (
            "atcs" in row
            and (isinstance(row["atcs"], list) or isinstance(row["atcs"], np.ndarray))
            and len(row["atcs"]) > 0
        ):
            for atc in row["atcs"]:
                if isinstance(atc, dict) and atc.get("atc_code"):
                    atc_codes.add(atc["atc_code"])

    logger.info(f"Found {len(atc_codes)} unique ATC codes in the data")

    existing_atcs = ATC.objects.filter(code__in=atc_codes)
    existing_codes = {atc.code for atc in existing_atcs}

    missing_codes = atc_codes - existing_codes
    if missing_codes:
        logger.warning(f"Missing ATC codes that will be skipped: {missing_codes}")

    atc_mapping = {atc.code: atc.id for atc in existing_atcs}
    return atc_mapping


@task()
def load_ont_form_routes(
    vmp_data: pd.DataFrame,
    route_mapping_data: pd.DataFrame,
    who_route_mapping: Dict[str, int],
) -> Dict[str, int]:
    """Replace all OntFormRoutes with new data"""
    logger = get_run_logger()

    dmd_to_who_route = {}
    for _, row in route_mapping_data.iterrows():
        dmd_to_who_route[row["dmd_ontformroute"]] = row["who_route"]

    route_entries = {}
    for _, row in vmp_data.iterrows():
        if (
            "ont_form_routes" in row
            and (
                isinstance(row["ont_form_routes"], list)
                or isinstance(row["ont_form_routes"], np.ndarray)
            )
            and len(row["ont_form_routes"]) > 0
        ):
            for route in row["ont_form_routes"]:
                if isinstance(route, dict) and route.get("route_name"):
                    route_entries[route["route_name"]] = route["route_code"]

    logger.info(f"Found {len(route_entries)} unique OntFormRoutes in the data")

    with transaction.atomic():
        logger.info("Deleting OntFormRoute records...")
        deleted_total = 0
        while OntFormRoute.objects.exists():
            ids = OntFormRoute.objects.values_list('id', flat=True)[:10000]
            batch_count = OntFormRoute.objects.filter(id__in=ids).delete()[0]
            deleted_total += batch_count
        logger.info(f"Finished deleting OntFormRoute records. Total deleted: {deleted_total}")

        ont_form_route_objects = []
        for route_name, route_code in route_entries.items():
            who_route_code = dmd_to_who_route.get(route_name)
            who_route_id = (
                who_route_mapping.get(who_route_code) if who_route_code else None
            )

            ont_form_route_objects.append(
                OntFormRoute(name=route_name, who_route_id=who_route_id)
            )

        created_objects = OntFormRoute.objects.bulk_create(
            ont_form_route_objects, batch_size=1000
        )
        logger.info(f"Created {len(created_objects)} OntFormRoute records")

        ont_form_route_mapping = {
            route.name: route.id for route in OntFormRoute.objects.all()
        }

    return ont_form_route_mapping


@task()
def load_vmps(
    vmp_data: pd.DataFrame,
    vtm_mapping: Dict[str, int],
    ingredient_mapping: Dict[str, int],
    atc_mapping: Dict[str, int],
    ont_form_route_mapping: Dict[str, int],
) -> None:
    """Replace all VMPs with new data"""
    logger = get_run_logger()

    with transaction.atomic():
        logger.info("Deleting VMP records...")
        deleted_total = 0
        while VMP.objects.exists():
            ids = VMP.objects.values_list('id', flat=True)[:10000]
            batch_count = VMP.objects.filter(id__in=ids).delete()[0]
            deleted_total += batch_count
        logger.info(f"Finished deleting VMP records. Total deleted: {deleted_total}")

        vmp_objects = []
        vmp_relationships = []

        for _, row in vmp_data.iterrows():
            vmp_code = row["vmp_code"]
            vtm_id = (
                vtm_mapping.get(row.get("vtm_code"))
                if pd.notna(row.get("vtm_code"))
                else None
            )

            # Use basis unit quantities where available, fallback to original
            udfs_value = row.get("udfs_basis_quantity") if pd.notna(row.get("udfs_basis_quantity")) else row.get("udfs")
            udfs_uom_value = row.get("udfs_basis_uom") if pd.notna(row.get("udfs_basis_uom")) else row.get("udfs_uom")
            unit_dose_uom_value = row.get("unit_dose_basis_uom") if pd.notna(row.get("unit_dose_basis_uom")) else row.get("unit_dose_uom")

            vmp_obj = VMP(
                code=vmp_code,
                name=row["vmp_name"],
                vtm_id=vtm_id,
                bnf_code=row.get("bnf_code"),
                df_ind=row.get("df_ind"),
                udfs=udfs_value,
                udfs_uom=udfs_uom_value,
                unit_dose_uom=unit_dose_uom_value,
            )
            vmp_objects.append(vmp_obj)

            relationships = {
                "vmp_code": vmp_code,
                "ingredient_ids": [],
                "ont_form_route_ids": [],
                "who_route_ids": [],
                "atc_ids": [],
            }

            if "ingredients" in row and (
                isinstance(row["ingredients"], list)
                or isinstance(row["ingredients"], np.ndarray)
            ):
                for ing in row["ingredients"]:
                    if isinstance(ing, dict):
                        ing_code = ing.get("ingredient_code")
                        if ing_code in ingredient_mapping:
                            relationships["ingredient_ids"].append(
                                ingredient_mapping[ing_code]
                            )

            if (
                "ont_form_routes" in row
                and (
                    isinstance(row["ont_form_routes"], list)
                    or isinstance(row["ont_form_routes"], np.ndarray)
                )
                and len(row["ont_form_routes"]) > 0
            ):
                for route in row["ont_form_routes"]:
                    if (
                        isinstance(route, dict)
                        and route.get("route_name") in ont_form_route_mapping
                    ):
                        relationships["ont_form_route_ids"].append(
                            ont_form_route_mapping[route["route_name"]]
                        )

            if relationships["ont_form_route_ids"]:
                who_routes = set(
                    OntFormRoute.objects.filter(
                        id__in=relationships["ont_form_route_ids"]
                    )
                    .exclude(who_route=None)
                    .values_list("who_route_id", flat=True)
                )
                relationships["who_route_ids"] = list(who_routes)

            if (
                "atcs" in row
                and (
                    isinstance(row["atcs"], list) or isinstance(row["atcs"], np.ndarray)
                )
                and len(row["atcs"]) > 0
            ):
                for atc in row["atcs"]:
                    if isinstance(atc, dict) and atc.get("atc_code") in atc_mapping:
                        relationships["atc_ids"].append(atc_mapping[atc["atc_code"]])

            vmp_relationships.append(relationships)

        created_objects = VMP.objects.bulk_create(vmp_objects, batch_size=1000)
        logger.info(f"Created {len(created_objects)} VMP records")

        vmp_lookup = {vmp.code: vmp for vmp in VMP.objects.all()}

        logger.info("Setting up many-to-many relationships...")

        ingredient_relations = []
        ont_form_route_relations = []
        who_route_relations = []
        atc_relations = []

        for rel in vmp_relationships:
            vmp = vmp_lookup[rel["vmp_code"]]
            
            for ingredient_id in rel["ingredient_ids"]:
                ingredient_relations.append(
                    VMP.ingredients.through(vmp_id=vmp.id, ingredient_id=ingredient_id)
                )
            
            for ont_form_route_id in rel["ont_form_route_ids"]:
                ont_form_route_relations.append(
                    VMP.ont_form_routes.through(vmp_id=vmp.id, ontformroute_id=ont_form_route_id)
                )
            
            for who_route_id in rel["who_route_ids"]:
                who_route_relations.append(
                    VMP.who_routes.through(vmp_id=vmp.id, whoroute_id=who_route_id)
                )
            
            for atc_id in rel["atc_ids"]:
                atc_relations.append(
                    VMP.atcs.through(vmp_id=vmp.id, atc_id=atc_id)
                )

        if ingredient_relations:
            VMP.ingredients.through.objects.bulk_create(ingredient_relations, batch_size=1000)
        if ont_form_route_relations:
            VMP.ont_form_routes.through.objects.bulk_create(ont_form_route_relations, batch_size=1000)
        if who_route_relations:
            VMP.who_routes.through.objects.bulk_create(who_route_relations, batch_size=1000)
        if atc_relations:
            VMP.atcs.through.objects.bulk_create(atc_relations, batch_size=1000)

        logger.info("Completed VMP creation and relationship setup")

@task()
def load_vmp_ingredient_strengths(
    vmp_data: pd.DataFrame,
    ingredient_mapping: Dict[str, int],
) -> None:
    """Load VMPIngredientStrength data using basis unit quantities where available"""
    logger = get_run_logger()
    
    with transaction.atomic():
        logger.info("Deleting VMPIngredientStrength records...")
        deleted_total = 0
        while VMPIngredientStrength.objects.exists():
            ids = VMPIngredientStrength.objects.values_list('id', flat=True)[:10000]
            batch_count = VMPIngredientStrength.objects.filter(id__in=ids).delete()[0]
            deleted_total += batch_count
        logger.info(f"Finished deleting VMPIngredientStrength records. Total deleted: {deleted_total}")

        vmp_lookup = {vmp.code: vmp for vmp in VMP.objects.all()}
        strength_objects = []

        for _, row in vmp_data.iterrows():
            vmp_code = row["vmp_code"]
            vmp = vmp_lookup.get(vmp_code)
            
            if not vmp:
                continue

            if "ingredients" in row and (
                isinstance(row["ingredients"], list)
                or isinstance(row["ingredients"], np.ndarray)
            ):
                for ing in row["ingredients"]:
                    if isinstance(ing, dict):
                        ing_code = ing.get("ingredient_code")
                        ingredient_id = ingredient_mapping.get(ing_code)
                        
                        if ingredient_id:
                            # Use basis unit quantities where available, fallback to original
                            strnt_nmrtr_val = (
                                ing.get("strnt_nmrtr_basis_val") 
                                if pd.notna(ing.get("strnt_nmrtr_basis_val"))
                                else ing.get("strnt_nmrtr_val")
                            )
                            strnt_nmrtr_uom_name = (
                                ing.get("strnt_nmrtr_basis_uom")
                                if pd.notna(ing.get("strnt_nmrtr_basis_uom"))
                                else ing.get("strnt_nmrtr_uom_name")
                            )
                            strnt_dnmtr_val = (
                                ing.get("strnt_dnmtr_basis_val")
                                if pd.notna(ing.get("strnt_dnmtr_basis_val"))
                                else ing.get("strnt_dnmtr_val")
                            )
                            strnt_dnmtr_uom_name = (
                                ing.get("strnt_dnmtr_basis_uom")
                                if pd.notna(ing.get("strnt_dnmtr_basis_uom"))
                                else ing.get("strnt_dnmtr_uom_name")
                            )

                            strength_obj = VMPIngredientStrength(
                                vmp=vmp,
                                ingredient_id=ingredient_id,
                                strnt_nmrtr_val=strnt_nmrtr_val,
                                strnt_nmrtr_uom_name=strnt_nmrtr_uom_name,
                                strnt_dnmtr_val=strnt_dnmtr_val,
                                strnt_dnmtr_uom_name=strnt_dnmtr_uom_name,
                                basis_of_strength_type=ing.get("basis_of_strength_type"),
                                basis_of_strength_name=ing.get("basis_of_strength_name"),
                            )
                            strength_objects.append(strength_obj)

        created_objects = VMPIngredientStrength.objects.bulk_create(
            strength_objects, batch_size=1000
        )
        logger.info(f"Created {len(created_objects)} VMPIngredientStrength records")


@task()
def vacuum_tables() -> None:
    logger = get_run_logger()

    vmp_vtm_tables = [
        "viewer_vmp",
        "viewer_vtm",
        "viewer_ingredient",
        "viewer_whoroute",
        "viewer_ontformroute",
        "viewer_vmpingredientstrength",
    ]

    m2m_tables = [
        "viewer_vmp_ingredients",
        "viewer_vmp_ont_form_routes",
        "viewer_vmp_who_routes",
        "viewer_vmp_atcs",
    ]

    cascaded_tables = [
        "viewer_scmdquantity",
        "viewer_dose",
        "viewer_ingredientquantity",
        "viewer_dddquantity",
        "viewer_indicativecost",
        "viewer_precomputedmeasure",
    ]

    all_affected_tables = vmp_vtm_tables + m2m_tables + cascaded_tables

    with connection.cursor() as cursor:
        for table in all_affected_tables:
            logger.info(f"Running VACUUM ANALYZE on {table}")
            cursor.execute(f"VACUUM ANALYZE {table}")

    logger.info(f"VACUUM ANALYZE completed on {len(all_affected_tables)} tables")


@flow(name="Load VMP and VTM data")
def load_vmp_vtm_data():
    logger = get_run_logger()
    logger.info("Starting VMP and VTM data load")

    vmp_data = extract_vmp_data()
    who_routes_data = extract_who_routes()
    route_mapping_data = extract_route_mapping()

    who_route_mapping = load_who_routes(who_routes_data)
    vtm_mapping = load_vtms(vmp_data)
    ingredient_mapping = load_ingredients(vmp_data)
    atc_mapping = validate_atcs(vmp_data)
    ont_form_route_mapping = load_ont_form_routes(
        vmp_data, route_mapping_data, who_route_mapping
    )

    load_vmps(
        vmp_data, vtm_mapping, ingredient_mapping, atc_mapping, ont_form_route_mapping
    )
    
    load_vmp_ingredient_strengths(vmp_data, ingredient_mapping)

    vacuum_tables()

    logger.info("VMP and VTM data load completed successfully")


if __name__ == "__main__":
    load_vmp_vtm_data()
