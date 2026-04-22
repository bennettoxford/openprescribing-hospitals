import csv
import math
import os
from collections import defaultdict
from datetime import datetime, timedelta
from django.conf import settings


from .models import Organisation, Region, CancerAlliance, DataStatus


def get_quantity_months():
    """Return sorted list of month date strings (YYYY-MM-DD) from DataStatus for dense array alignment."""
    dates = list(
        DataStatus.objects.order_by('year_month').values_list('year_month', flat=True)
    )
    return [d.strftime('%Y-%m-%d') for d in dates]


def get_organisation_data():
    """
    Get standardised organisation data for NHS organisations.

    Returns:
        dict with keys:
            orgs: ODS code -> organisation name (successors only, predecessors merged)
            org_codes: organisation name -> ODS code (successors only)
            trust_types: organisation name -> trust type (e.g. "Acute", "Mental Health")
            org_regions: organisation name -> NHS region name
            org_icbs: organisation name -> ICB name
            org_cancer_alliances: organisation name -> Cancer Alliance name
            regions_hierarchy: list of dicts with region, region_code, and icbs
            cancer_alliances: list of dicts with name and code for filter dropdown
    """
    orgs = Organisation.objects.select_related(
        'successor', 'trust_type', 'region', 'icb', 'cancer_alliance'
    ).values(
        'ods_code', 'ods_name', 'successor__ods_name', 'trust_type__name',
        'region__name', 'region__code', 'icb__name', 'icb__code',
        'cancer_alliance__name', 'cancer_alliance__code'
    ).order_by('ods_name')

    org_names = {}
    org_codes = {}
    trust_types = {}
    org_regions = {}
    org_icbs = {}
    org_cancer_alliances = {}

    for org in orgs:
        name = org['ods_name']
        code = org['ods_code']

        if org['successor__ods_name']:
            continue

        org_names[code] = name
        org_codes[name] = code
        if org.get('trust_type__name'):
            trust_types[name] = org['trust_type__name']
        if org.get('region__name'):
            org_regions[name] = org['region__name']
        if org.get('icb__name'):
            org_icbs[name] = org['icb__name']
        if org.get('cancer_alliance__name'):
            org_cancer_alliances[name] = org['cancer_alliance__name']

    # ICBs that have at least one successor
    icb_ids_with_successor_orgs = set(
        Organisation.objects.filter(
            successor__isnull=True,
            icb__isnull=False,
        ).values_list('icb_id', flat=True).distinct()
    )

    regions_hierarchy = []
    for region in Region.objects.prefetch_related('icbs').order_by('name'):
        icbs = [
            {'name': icb.name, 'code': icb.code}
            for icb in region.icbs.all().order_by('name')
            if icb.id in icb_ids_with_successor_orgs
        ]
        regions_hierarchy.append({
            'region': region.name,
            'region_code': region.code or '',
            'icbs': icbs,
        })

    cancer_alliances = [
        {'name': ca.name, 'code': ca.code or ''}
        for ca in CancerAlliance.objects.all().order_by('name')
    ]

    return {
        'orgs': org_names,
        'org_codes': org_codes,
        'trust_types': trust_types,
        'org_regions': org_regions,
        'org_icbs': org_icbs,
        'org_cancer_alliances': org_cancer_alliances,
        'regions_hierarchy': regions_hierarchy,
        'cancer_alliances': cancer_alliances,
    }


def safe_float(value):
    """Convert value to float, handling NaN, inf, and None values."""
    if value is None:
        return None
    try:
        float_val = float(value)
        if math.isnan(float_val) or math.isinf(float_val):
            return None
        return float_val
    except (ValueError, TypeError):
        return None


def format_ddd_unit_label(ddd_entries):
    """Format DDD metadata as a display label like 'DDD (1.0 mg)'."""
    unique_entries = []
    seen = set()

    for ddd_value, unit_type in ddd_entries:
        if ddd_value is None or not unit_type:
            continue

        entry = f"{ddd_value} {unit_type}"
        if entry in seen:
            continue

        seen.add(entry)
        unique_entries.append(entry)

    if not unique_entries:
        return None

    return f"DDD ({' | '.join(unique_entries)})"


def get_ddd_unit_map(vmp_ids):
    """Return VMP id -> formatted DDD unit label."""
    from .models import DDD

    ddd_entries_by_vmp = defaultdict(list)
    ddd_rows = (
        DDD.objects.filter(vmp_id__in=vmp_ids)
        .values_list("vmp_id", "ddd", "unit_type")
        .order_by("vmp_id", "ddd", "unit_type")
    )

    for vmp_id, ddd_value, unit_type in ddd_rows:
        ddd_entries_by_vmp[vmp_id].append((ddd_value, unit_type))

    labels = {}
    for vmp_id, ddd_entries in ddd_entries_by_vmp.items():
        label = format_ddd_unit_label(ddd_entries)
        if label:
            labels[vmp_id] = label

    return labels


def generate_dummy_data_csv():
    start_date = datetime(2020, 11, 1)
    ods_codes = [
        {"code": "RK9", "name": "University Hospitals Plymouth NHS Trust"},
        {"code": "RT2", "name": "Pennine Care NHS Foundation Trust"},
        {
            "code": "RXY",
            "name": "Kent And Medway NHS And Social Care Partnership Trust",
        },
    ]
    medications = [
        {
            "vmp_code": "42206511000001102",
            "vmp_name": "Apixaban 5mg tablets",
            "base_quantity": 1000,
        },
        {
            "vmp_code": "14254711000001104",
            "vmp_name": "Rivaroxaban 10mg tablets",
            "base_quantity": 38,
        },
    ]

    csv_path = os.path.join(settings.STATIC_ROOT, "dummy_data.csv")

    with open(csv_path, "w", newline="") as csvfile:
        fieldnames = [
            "year_month",
            "vmp_code",
            "vmp_name",
            "ods_code",
            "ods_name",
            "SCMD_quantity",
            "SCMD_quantity_basis",
            "dose_quantity",
            "converted_udfs",
            "udfs_basis",
            "dose_unit",
            "df_ind",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(6):
            current_date = start_date + timedelta(days=30 * i)
            for ods in ods_codes:
                for med in medications:
                    quantity = (
                        med["base_quantity"]
                        + (i * 10)
                        + (50 if ods["code"] == "RK9" else 0)
                    )
                    writer.writerow(
                        {
                            "year_month": current_date.strftime("%Y-%m-%d"),
                            "vmp_code": med["vmp_code"],
                            "vmp_name": med["vmp_name"],
                            "ods_code": ods["code"],
                            "ods_name": ods["name"],
                            "SCMD_quantity": quantity,
                            "SCMD_quantity_basis": "tablet",
                            "dose_quantity": quantity,
                            "converted_udfs": 1.0,
                            "udfs_basis": "tablet",
                            "dose_unit": "tablet",
                            "df_ind": "Discrete",
                        }
                    )

    print(f"CSV file generated at: {csv_path}")
