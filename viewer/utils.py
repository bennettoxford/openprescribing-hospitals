import csv
import math
import re
import unicodedata
from datetime import datetime, timedelta
from django.conf import settings
import os


from .models import Organisation


def normalise_string(s):
    """Lowercase, NFD normalise, strip accents, remove punctuation, normalise whitespace."""
    if s is None or not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"""['".,\/#!$%\^&\*;:{}=\-_`~()]""", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def tokenize(s):
    """Normalise then split on whitespace; return list of non-empty tokens."""
    normalised = normalise_string(s)
    return normalised.split() if normalised else []


def _token_matches(search_token, item_tokens):
    """True if any item token equals or contains the search token."""
    return any(
        search_token == it or (len(it) >= len(search_token) and search_token in it)
        for it in item_tokens
    )


def coverage_score(search_tokens, item_tokens):
    """Fraction of search tokens that match (0..1)."""
    if not search_tokens:
        return 1.0
    matched = sum(1 for st in search_tokens if _token_matches(st, item_tokens))
    return matched / len(search_tokens)


def get_organisation_data():
    """
    Get standardised organisation data.
    
    Returns:
        dict: Contains orgs (code->name mapping), org_codes (name->code mapping), 
              and predecessor_map (successor_name->list of predecessor names)
    """
    
    orgs = Organisation.objects.select_related('successor').values(
        'ods_code', 'ods_name', 'successor__ods_name'
    ).order_by('ods_name')
    
    org_names = {}
    org_codes = {}
    predecessor_map = {}
    
    for org in orgs:
        name = org['ods_name']
        code = org['ods_code']
        
        org_names[code] = name
        org_codes[name] = code
        
        if org['successor__ods_name']:
            successor_name = org['successor__ods_name']
            if successor_name not in predecessor_map:
                predecessor_map[successor_name] = []
            predecessor_map[successor_name].append(name)
    
    return {
        'orgs': org_names,
        'org_codes': org_codes,
        'predecessor_map': predecessor_map
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
