from datetime import date

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection
from django.test import override_settings

from viewer.management.commands.load_sample_data import SAMPLE_MEASURES, _rows, _series
from viewer.models import (
    DDD,
    DataStatus,
    Ingredient,
    Measure,
    MeasureVMP,
    Organisation,
    OrgSubmissionCache,
    Region,
    SCMDQuantity,
    TrustAdmission,
    VMP,
    VTM,
)
from viewer.search import (
    search_atc_results,
    search_ingredient_results,
    search_product_results,
)


def _result_codes(results):
    codes = set()
    for result in results:
        codes.add(result["code"])
        for child in result.get("vmps", []):
            codes.add(child["code"])
    return codes


def _positive_vmp_ids(organisation, index):
    found = set()
    for quantity in SCMDQuantity.objects.filter(organisation=organisation):
        data = quantity.data or []
        if index >= len(data):
            continue
        value = data[index]
        if isinstance(value, (int, float)) and value > 0:
            found.add(quantity.vmp_id)
    return found


def _assert_successor_submission(months):
    predecessor = (
        Organisation.objects.exclude(successor=None)
        .select_related("successor")
        .order_by("ods_code")
        .first()
    )
    assert predecessor is not None
    successor = predecessor.successor
    rolled_index = next(
        (
            index
            for index in range(len(months))
            if _positive_vmp_ids(predecessor, index)
        ),
        None,
    )
    assert rolled_index is not None
    expected_ids = _positive_vmp_ids(successor, rolled_index)
    for other in successor.predecessors.all():
        expected_ids |= _positive_vmp_ids(other, rolled_index)
    cache = OrgSubmissionCache.objects.get(
        organisation=successor,
        month=months[rolled_index],
    )
    assert cache.has_submitted
    assert cache.vmp_count == len(expected_ids)
    assert cache.quantity_count == len(expected_ids)


def _table_counts():
    return {
        "vmp": VMP.objects.count(),
        "organisation": Organisation.objects.count(),
        "scmd": SCMDQuantity.objects.count(),
        "admission": TrustAdmission.objects.count(),
        "ddd": DDD.objects.count(),
        "month": DataStatus.objects.count(),
        "submission": OrgSubmissionCache.objects.count(),
        "measure_vmp": MeasureVMP.objects.count(),
    }


@pytest.mark.django_db(transaction=True)
@override_settings(DEBUG=True)
def test_load_sample_data_matches_the_sample_files():
    stale_vtm = VTM.objects.create(vtm="STALE-VTM", name="Stale")
    VMP.objects.create(code="STALE-VMP", name="Stale product", vtm=stale_vtm)
    stale_region = Region.objects.create(code="STALE-R", name="Stale region")
    Organisation.objects.create(
        ods_code="STALE",
        ods_name="Stale trust",
        region=stale_region,
    )
    Ingredient.objects.create(code="STALE-ING", name="Stale ingredient")
    if "django_cache_table" not in connection.introspection.table_names():
        call_command("createcachetable")

    call_command("load_sample_data")
    assert _table_counts()["vmp"] == len(_rows("products.csv"))

    products = {row["vmp_code"]: row for row in _rows("products.csv")}
    trusts = {row["ods_code"]: row for row in _rows("trusts.csv")}
    month_rows = _rows("data_status.csv")
    ingredients = _rows("product_ingredients.csv")

    vmps = {vmp.code: vmp for vmp in VMP.objects.prefetch_related("ingredients")}
    assert set(vmps) == set(products)
    for code, row in products.items():
        assert vmps[code].name == row["vmp_name"]

    expected_ingredients = {}
    for row in ingredients:
        expected_ingredients.setdefault(row["vmp_code"], set()).add(row["ingredient_code"])
    for code, vmp in vmps.items():
        assert {item.code for item in vmp.ingredients.all()} == expected_ingredients.get(
            code, set()
        )
    assert set(Ingredient.objects.values_list("code", flat=True)) == {
        code for codes in expected_ingredients.values() for code in codes
    }

    orgs = {org.ods_code: org for org in Organisation.objects.all()}
    assert set(orgs) == set(trusts)
    for code, row in trusts.items():
        assert orgs[code].ods_name == row["ods_name"]
    assert orgs["RGT"].in_shelford_group is True
    assert orgs["RF4"].in_shelford_group is False
    assert set(Region.objects.values_list("code", flat=True)) == {
        row["region_code"] for row in trusts.values() if row["region_code"]
    }

    months = list(
        DataStatus.objects.order_by("year_month").values_list("year_month", flat=True)
    )
    assert [month.isoformat() for month in months] == [
        row["year_month"] for row in month_rows
    ]

    dose_rows = _rows("dose.csv")
    quantity_keys = {
        (row["vmp_code"], row["ods_code"])
        for row in dose_rows
        if row["vmp_code"]
        and row["ods_code"]
        and row["scmd_quantity"]
        and row["scmd_basis_unit_name"]
    }
    assert SCMDQuantity.objects.count() == len(quantity_keys)
    adrenaline = SCMDQuantity.objects.get(
        vmp__code="20122411000001103",
        organisation__ods_code="R0A",
    )
    assert adrenaline.data[0] == pytest.approx(7160)

    admission_rows = [row for row in _rows("admissions.csv") if row["count"]]
    assert TrustAdmission.objects.count() == len(admission_rows)
    manchester = TrustAdmission.objects.get(
        organisation__ods_code="R0A",
        period=date(2024, 8, 1),
    )
    assert manchester.count == 23480

    ddd_rows = _rows("ddd.csv")
    assert DDD.objects.count() == len(ddd_rows)
    adrenaline_ddd = DDD.objects.select_related("who_route").get(
        vmp__code="20122411000001103"
    )
    assert adrenaline_ddd.ddd == pytest.approx(0.5)
    assert adrenaline_ddd.unit_type == "mg"
    assert adrenaline_ddd.who_route.code == "P"

    assert OrgSubmissionCache.objects.count() == len(orgs) * len(months)
    _assert_successor_submission(months)

    assert "39732411000001106" in _result_codes(
        search_product_results("Amoxicillin 500mg capsules")
    )
    assert any(
        item["code"] == "387362001"
        for item in search_ingredient_results("Adrenaline")
    )
    assert any(
        item["code"] == "A02BC" for item in search_atc_results("Proton Pump Inhibitors")
    )

    loaded_codes = set(vmps)
    for slug in SAMPLE_MEASURES:
        measure = Measure.objects.get(slug=slug)
        measure_codes = set(
            MeasureVMP.objects.filter(measure=measure).values_list(
                "vmp__code", flat=True
            )
        )
        assert measure_codes
        assert measure_codes <= loaded_codes


@pytest.mark.django_db
@override_settings(DEBUG=False)
def test_load_sample_data_refuses_when_debug_is_false():
    VTM.objects.create(vtm="KEEP-VTM", name="Keep")

    with pytest.raises(CommandError, match="DEBUG is true"):
        call_command("load_sample_data")

    assert VTM.objects.filter(vtm="KEEP-VTM").exists()


def test_series_rejects_a_null_key():
    with pytest.raises(CommandError, match="vmp_code"):
        _series(
            [
                {
                    "vmp_code": "",
                    "ods_code": "R0A",
                    "year_month": "2024-08-01",
                    "scmd_quantity": "1",
                }
            ],
            ("vmp_code", "ods_code"),
            "year_month",
            "scmd_quantity",
            [date(2024, 8, 1)],
        )


def test_series_skips_a_null_quantity():
    assert (
        _series(
            [
                {
                    "vmp_code": "1",
                    "ods_code": "R0A",
                    "year_month": "2024-08-01",
                    "scmd_quantity": "",
                }
            ],
            ("vmp_code", "ods_code"),
            "year_month",
            "scmd_quantity",
            [date(2024, 8, 1)],
        )
        == {}
    )
