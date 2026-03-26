from collections import Counter

from google.cloud import bigquery
from prefect import flow, get_run_logger

from pipeline.setup.bq_tables import VMP_MANUAL_WHO_ROUTE_TABLE_SPEC
from pipeline.utils.utils import get_bigquery_client

# Curated WHO route for DDD when dm+d maps a VMP to multiple WHO administration routes.
VMP_MANUAL_WHO_ROUTE_MAPPINGS = [
    {"vmp_code": "39709011000001102", "vmp_name": "Progesterone 400mg pessaries", "who_route_code": "V"},
    {"vmp_code": "39708911000001106", "vmp_name": "Progesterone 200mg pessaries", "who_route_code": "V"},
    {"vmp_code": "36015811000001103", "vmp_name": "Phytomenadione 2mg/0.2ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "4078411000001107", "vmp_name": "Paraldehyde 100% solution for injection 5ml ampoules", "who_route_code": "P"},
    {"vmp_code": "41760411000001106", "vmp_name": "Colistimethate 500,000unit powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "4638311000001106", "vmp_name": "Colistimethate 2million unit powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "41760311000001104", "vmp_name": "Colistimethate 1million unit powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "20532611000001105", "vmp_name": "Caffeine citrate 20mg/1ml solution for infusion ampoules", "who_route_code": "P"},
    {"vmp_code": "13821411000001101", "vmp_name": "Caffeine citrate 10mg/1ml solution for infusion ampoules", "who_route_code": "P"},
    {"vmp_code": "39712911000001106", "vmp_name": "Sodium polystyrene sulfonate powder sugar free", "who_route_code": "O"},
    {"vmp_code": "19223111000001102", "vmp_name": "Calcium polystyrene sulfonate powder sugar free", "who_route_code": "O"},
    {"vmp_code": "19223011000001103", "vmp_name": "Calcium polystyrene sulfonate powder", "who_route_code": "O"},
    {"vmp_code": "41956611000001106", "vmp_name": "Vancomycin 500mg powder for solution for infusion vials", "who_route_code": "P"},
    {"vmp_code": "41955711000001105", "vmp_name": "Teicoplanin 400mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "41950111000001101", "vmp_name": "Flucloxacillin 1g powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "41881011000001107", "vmp_name": "Pentamidine 300mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "4700511000001101", "vmp_name": "Doxorubicin 50mg/25ml solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "36125511000001104", "vmp_name": "Midazolam 5mg/5ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "35915111000001103", "vmp_name": "Teicoplanin 200mg powder and solvent for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "41792311000001102", "vmp_name": "Mitomycin 10mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "41795111000001106", "vmp_name": "Thiotepa 15mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "36072411000001103", "vmp_name": "Epirubicin 10mg/5ml solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "4700611000001102", "vmp_name": "Doxorubicin 10mg/5ml solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "10919611000001103", "vmp_name": "Epirubicin 100mg/50ml solution for infusion vials", "who_route_code": "P"},
    {"vmp_code": "41786311000001104", "vmp_name": "Epirubicin 10mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "35601611000001106", "vmp_name": "Doxorubicin 100mg/50ml solution for infusion vials", "who_route_code": "P"},
    {"vmp_code": "36125011000001107", "vmp_name": "Midazolam 10mg/5ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "41785011000001106", "vmp_name": "Cyclophosphamide 500mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "24856311000001103", "vmp_name": "Hydroxocobalamin 10mg/2ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "41792511000001108", "vmp_name": "Mitomycin 2mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "23344411000001103", "vmp_name": "Ergocalciferol 600,000units/1.5ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "36124911000001107", "vmp_name": "Midazolam 10mg/2ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "41786511000001105", "vmp_name": "Epirubicin 50mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "41956411000001108", "vmp_name": "Vancomycin 1g powder for solution for infusion vials", "who_route_code": "P"},
    {"vmp_code": "41784711000001109", "vmp_name": "Cyclophosphamide 200mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "41950511000001105", "vmp_name": "Flucloxacillin 500mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "36071311000001101", "vmp_name": "Doxorubicin 200mg/100ml solution for infusion vials", "who_route_code": "P"},
    {"vmp_code": "36041911000001101", "vmp_name": "Mesna 1g/10ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "41785911000001105", "vmp_name": "Doxorubicin 50mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "7977911000001102", "vmp_name": "Midazolam 100mg/50ml solution for infusion vials", "who_route_code": "P"},
    {"vmp_code": "41950211000001107", "vmp_name": "Flucloxacillin 250mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "41786411000001106", "vmp_name": "Epirubicin 20mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "36125411000001103", "vmp_name": "Midazolam 50mg/10ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "36125211000001102", "vmp_name": "Midazolam 2mg/2ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "35915211000001109", "vmp_name": "Teicoplanin 400mg powder and solvent for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "20293611000001101", "vmp_name": "Epirubicin 20mg/10ml solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "41955611000001101", "vmp_name": "Teicoplanin 200mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "32464311000001101", "vmp_name": "Isoniazid 100mg/5ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "27957911000001102", "vmp_name": "Dexamethasone (base) 3.8mg/1ml solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "36042011000001108", "vmp_name": "Mesna 400mg/4ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "36072611000001100", "vmp_name": "Epirubicin 50mg/25ml solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "4508111000001108", "vmp_name": "Midazolam 50mg/50ml solution for infusion vials", "who_route_code": "P"},
    {"vmp_code": "4052711000001101", "vmp_name": "Dexamethasone 4mg/1ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "22154711000001106", "vmp_name": "Midazolam 5mg/2ml solution for injection ampoules", "who_route_code": "P"},
    {"vmp_code": "35601211000001109", "vmp_name": "Doxorubicin 20mg/10ml solution for infusion vials", "who_route_code": "P"},
    {"vmp_code": "4052811000001109", "vmp_name": "Dexamethasone 8mg/2ml solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "41784611000001100", "vmp_name": "Cyclophosphamide 1g powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "41792411000001109", "vmp_name": "Mitomycin 20mg powder for solution for injection vials", "who_route_code": "P"},
    {"vmp_code": "36072511000001104", "vmp_name": "Epirubicin 200mg/100ml solution for infusion vials", "who_route_code": "P"},
]


@flow(name="Import VMP manual WHO route mappings")
def import_vmp_manual_who_route():
    """Load manual VMP → WHO route rows for DDD disambiguation into BigQuery."""
    logger = get_run_logger()
    client = get_bigquery_client()

    assert not (dupes := sorted(c for c, n in Counter(str(r["vmp_code"]) for r in VMP_MANUAL_WHO_ROUTE_MAPPINGS).items() if n > 1)), \
        "Duplicate vmp_code in VMP_MANUAL_WHO_ROUTE_MAPPINGS: " + ", ".join(dupes)

    records = [
        {
            "vmp_code": str(r["vmp_code"]),
            "vmp_name": str(r["vmp_name"]) if r.get("vmp_name") else None,
            "who_route_code": str(r["who_route_code"]),
        }
        for r in VMP_MANUAL_WHO_ROUTE_MAPPINGS
    ]

    job_config = bigquery.LoadJobConfig(
        schema=VMP_MANUAL_WHO_ROUTE_TABLE_SPEC.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    logger.info(
        f"Loading {len(records)} manual WHO route mappings to {VMP_MANUAL_WHO_ROUTE_TABLE_SPEC.full_table_id}"
    )
    job = client.load_table_from_json(
        records,
        VMP_MANUAL_WHO_ROUTE_TABLE_SPEC.full_table_id,
        job_config=job_config,
    )
    job.result()
    logger.info(
        f"Successfully loaded {len(records)} records to {VMP_MANUAL_WHO_ROUTE_TABLE_SPEC.full_table_id}"
    )


if __name__ == "__main__":
    import_vmp_manual_who_route()
