from google.cloud import bigquery
from typing import Dict, List
from prefect import task, flow, get_run_logger
from pipeline.setup.config import PROJECT_ID, DATASET_ID
from pipeline.utils.utils import get_bigquery_client
from pipeline.setup.bq_tables import (
    ADM_ROUTE_MAPPING_TABLE_SPEC,
    WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC,
    DMD_TABLE_SPEC,
)


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
        "null": "Null",
    }

    return who_routes_of_administration


@task
def get_distinct_dmd_routes() -> List[str]:
    """Fetch distinct routes from DMD table"""
    logger = get_run_logger()
    client = get_bigquery_client()

    query = f"""
    SELECT DISTINCT ontformroute_descr
    FROM `{PROJECT_ID}.{DATASET_ID}.dmd`,
    UNNEST(ontformroutes)
    WHERE ontformroute_descr IS NOT NULL
    ORDER BY ontformroute_descr
    """

    results = client.query(query).result()
    dmd_routes = [row.ontformroute_descr for row in results]
    logger.info(f"Found {len(dmd_routes)} distinct routes in DMD table")
    return dmd_routes


@task
def create_adm_route_mapping(dmd_routes: List[str]) -> Dict:
    """
    Create a mapping function that applies the same rule-based logic as the SQL query
    to determine the WHO route code for a given DMD route.
    """
    logger = get_run_logger()

    def map_dmd_to_who_route(dmd_route):
        exact_matches = {
            "implant.subcutaneous": "implant",
            "pressurizedinhalation.inhalation": "Inhal.aerosol",
            "powderinhalation.inhalation": "Inhal.powder",
            "solutionnebuliser.inhalation": "Inhal.solution",
            "dispersionnebuliser.inhalation": "Inhal.solution",  # amikacin liposomal
            "inhalationsolution.inhalation": "Inhal.solution",  # mist inhalers - e.g. tiotropium respimat
            "suspensionnebuliser.inhalation": "Inhal.solution",  # steroid nebs
            "medicatedchewing-gum.oromucosal": "Chewing gum",  # nicotine
            "solution.oromucosal": "SL",  # midazolam oromucosal
            "tabletsublingual.oromucosal": "SL",
            "lozenge.oromucosal": "SL",  # fentanyl lozenge
            "lyophilisate.oromucosal": "SL",  # buprenorphine
            "filmsublingual.oromucosal": "SL",  # buprenorphine
            "vapour.oromucosal": "Inhal",  # nicotine inhalator - to match ATC
        }

        if dmd_route in exact_matches:
            return exact_matches[dmd_route], "mapped"

        excluded_patterns = [
            ".auricular",  # eye drops don't seem to have DDDs
            ".bodycavity",  # ??
            ".cutaneous",  # no DDDs
            ".dental",  # mostly sodium fluoride toothpaste, chlorhexidine mouthwash - no obvious route match
            ".endocervical",  # no obvious route match
            ".endosinusial",  # no obvious route match
            ".endotracheopulmonary",  # possibly match on inhalation, but not much here with DDDs
            ".epidural",  # no obvious route match/ no relevent DDDs e.g. bupivacaine
            ".epilesional",  # no obvious route match
            ".extraamniotic",  # no obvious route match
            ".extracorporeal",  # no obvious route match
            ".gingival",  # no obvious route match
            ".haemodialysis",  # no obvious route match
            ".haemofiltration",  # no obvious route match
            ".implantation",  # no obvious route match. Not to be confused with subcutaneous implants
            ".infiltration",  # no obvious route match
            ".scalp",  # no obvious route match
            ".ophthalmic",  # no obvious route match
            ".opthalmic",  # typo for above
            ".intestinal",  # levodopa gels for Parkinson's disease
            ".intraarterial",  # contrast media. A few drugs but the probably also have a more common P route.
            ".intraarticular",  # limited interest
            ".intrabursal",  # limited interest
            ".intracameral",  # niche
            ".intracerebroventricular",  # niche
            ".intracholangiopancreatic",  # contrast media
            ".intradermal",  # limited interest - a few vaccines - probably no DDDs
            ".intradiscal",  # niche
            ".intraepidermal",  # skin prick tests
            ".intraglandular",  # contrast / botox
            ".intralesional",  # contrast media. A few drugs but the probably also have a more common P route.
            ".intraocular",  # no obvious route match
            ".intraosseous",  # A few specific drugs but the probably also have a more common P route.
            ".intraperitoneal",  # no obvious route match
            ".intrapleural",  # niche
            ".intraputaminal",  # putamen = part of brain. Very niche - 1 product - gene therapy
            ".intrathecal",  # A few specific drugs but the probably also have a more common P route.
            ".intratumoral",  # niche
            ".intrauterine",  # no obvious route match
            ".intraventricular cardiac",  # niche
            ".intravitreal",  # no obvious route. No DDDs for those of interest.
            ".iontophoresis",  # no obvious route
            ".linelock",  # no obvious route
            ".ocular",  # no obvious route
            ".periarticular",  # no obvious route
            ".peribulbar ocular",  # no obvious route
            ".perilesional",  # no obvious route
            ".perineural",  # no obvious route
            ".peritumoral",  # no obvious route
            ".regionalperfusion",  # no obvious route
            ".subconjunctival",  # no obvious route
            ".submucosal",  # no obvious route, small number of products with a more common P route
            ".subretinal",  # no obvious route
        ]

        excluded_exact_routes = [
            "gasinhalation.inhalation",  # medical gases
            "vapourinhalationliquid.inhalation",  # inhaled anaesthetics
            "vapourinhalation.inhalation",  # only 1 product - Benzoin compound tincture
            "impregnatedcigarette.inhalation",  # no DDDs
            "gargle.oromucosal",  # no DDDs
            "gel.oromucosal",  # no DDDs
            "homeopathicpillule.oromucosal",  # no DDDs
            "liquid.oromucosal",  # no DDDs
            "liquidspray.oromucosal",  # no DDDs
            "mouthwash.oromucosal",  # no DDDs
            "paste.oromucosal",  # no DDDs
            "homeopathictablet.oromucosal",  # no DDDs
            "pastille.oromucosal",  # no DDDs
            "solutionspray.oromucosal",  # no DDDs
            "suspension.oromucosal",  # no DDDs
            "suspensionspray.oromucosal",  # no DDDs
            "tabletmuco-adhesive.oromucosal",  # no DDDs
            "vapour.oromucosal",  # no DDDs
        ]

        if dmd_route in excluded_exact_routes:
            return None, "excluded"

        if any(pattern in dmd_route for pattern in excluded_patterns):
            return None, "excluded"

        if dmd_route.endswith(".oral"):
            return "O", "mapped"
        elif dmd_route.endswith(".gastroenteral"):
            return "O", "mapped"
        elif dmd_route.endswith(".intravenous"):
            return "P", "mapped"
        elif (
            ".intramuscular" in dmd_route
        ):  # so it includes intramuscular and intramuscular-deep
            return "P", "mapped"
        elif dmd_route.endswith(".subcutaneous"):
            return "P", "mapped"
        elif dmd_route.endswith(".intracavernous"):
            return "P", "mapped"
        elif dmd_route.endswith(".intracardiac"):
            return "P", "mapped"
        elif dmd_route.endswith(".intracoronary"):
            return "P", "mapped"
        elif dmd_route.endswith(".vaginal"):
            return "V", "mapped"
        elif dmd_route.endswith(".rectal"):
            return "R", "mapped"
        elif dmd_route.endswith(".submucosalrectal"):
            return "R", "mapped"
        elif dmd_route.endswith(".nasal"):
            return "N", "mapped"
        elif dmd_route.endswith(".buccal"):
            return "SL", "mapped"
        elif dmd_route.endswith(".sublingual"):
            return "SL", "mapped"
        elif dmd_route.endswith(".transdermal"):
            return "TD", "mapped"
        elif dmd_route.endswith(".urethral"):
            return "urethral", "mapped"
        elif dmd_route.endswith(".intravesical"):
            return "intravesical", "mapped"

        return None, "unmapped"

    dmd_who_route_mapping = {}
    unmapped_routes = []
    excluded_routes = []

    for route in dmd_routes:
        who_route, status = map_dmd_to_who_route(route)

        if status == "mapped":
            dmd_who_route_mapping[route] = who_route
        elif status == "excluded":
            excluded_routes.append(route)
            dmd_who_route_mapping[route] = None
        else:
            unmapped_routes.append(route)
            dmd_who_route_mapping[route] = None

    logger.info(f"Successfully mapped {len(dmd_who_route_mapping)} routes")
    logger.info(f"Excluded {len(excluded_routes)} routes by design")

    if unmapped_routes:
        error_msg = f"Found {len(unmapped_routes)} routes that could not be mapped and are not in exclusion list: {unmapped_routes}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    return dmd_who_route_mapping


@task
def import_who_routes_of_administration(route_mapping: Dict) -> List[Dict]:
    """Convert route mapping dictionary to a list of records"""
    logger = get_run_logger()
    client = get_bigquery_client()

    route_records = []

    for who_route_code, who_route_description in route_mapping.items():
        record = {
            "who_route_code": who_route_code,
            "who_route_description": who_route_description,
        }
        route_records.append(record)

    job_config = bigquery.LoadJobConfig(
        schema=WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC.schema,
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info(f"Uploading {len(route_records)} route mapping records to BigQuery...")
    job = client.load_table_from_json(
        route_records,
        WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC.full_table_id,
        job_config=job_config,
    )
    job.result()
    logger.info(
        f"Successfully imported {len(route_records)} route mapping records to {WHO_ROUTES_OF_ADMINISTRATION_TABLE_SPEC.full_table_id}"
    )


@task
def import_mapping(route_mapping: Dict) -> List[Dict]:
    """Convert route mapping dictionary to a list of records"""
    logger = get_run_logger()
    client = get_bigquery_client()

    route_records = []

    for dmd_route, who_route in route_mapping.items():
        record = {"dmd_ontformroute": dmd_route, "who_route": who_route}
        route_records.append(record)

    job_config = bigquery.LoadJobConfig(
        schema=ADM_ROUTE_MAPPING_TABLE_SPEC.schema,
        write_disposition="WRITE_TRUNCATE",
    )

    logger.info(f"Uploading {len(route_records)} route mapping records to BigQuery...")
    job = client.load_table_from_json(
        route_records, ADM_ROUTE_MAPPING_TABLE_SPEC.full_table_id, job_config=job_config
    )
    job.result()
    logger.info(
        f"Successfully imported {len(route_records)} route mapping records to {ADM_ROUTE_MAPPING_TABLE_SPEC.full_table_id}"
    )

@task
def validate_routes():
    """Validate that all routes in DMD exist in route mapping table"""
    logger = get_run_logger()
    client = get_bigquery_client()

    query = f"""
    WITH unique_routes AS (
        SELECT DISTINCT ontformroute_descr
        FROM `{DMD_TABLE_SPEC.full_table_id}`,
        UNNEST(ontformroutes)
    )
    SELECT ontformroute_descr
    FROM unique_routes
    WHERE ontformroute_descr NOT IN (
        SELECT dmd_ontformroute
        FROM `{ADM_ROUTE_MAPPING_TABLE_SPEC.full_table_id}`
    )
    """

    results = client.query(query).result()
    missing_routes = [row.ontformroute_descr for row in results]

    if missing_routes:
        logger.error(
            f"Found routes in DMD that are not in route mapping table: {missing_routes}"
        )
        return {"valid": False, "missing_routes": missing_routes}

    logger.info("All DMD routes are present in route mapping table")
    return {"valid": True}

@flow(name="Import Administration Route Mapping")
def import_adm_route_mapping():

    who_routes = create_who_routes_of_administration()
    who_result = import_who_routes_of_administration(who_routes)

    dmd_routes = get_distinct_dmd_routes()
    adm_routes = create_adm_route_mapping(dmd_routes)
    adm_result = import_mapping(adm_routes)

    validate_routes()


if __name__ == "__main__":
    import_adm_route_mapping()
