import json
import pytest
from datetime import date
from django.contrib.auth.models import User
from django.test import Client
from django.urls import reverse

from viewer.models import (
    Region,
    ICB,
    Organisation,
    VMP,
    VTM,
    DDDQuantity,
    DataStatus,
    Ingredient,
    Measure,
    MeasureVMP,
    PrecomputedMeasure,
    PrecomputedPercentile,
)
from viewer.search import MAX_ANALYSIS_VMP_COUNT


@pytest.fixture
def region():
    return Region.objects.create(name="Test Region", code="TR")


@pytest.fixture
def icb(region):
    return ICB.objects.create(code="QXX", name="Test ICB", region=region)


@pytest.fixture
def predecessor_successor_orgs(region, icb):
    successor = Organisation.objects.create(
        ods_code="SUC",
        ods_name="Successor Trust",
        region=region,
        icb=icb,
        successor=None,
    )
    predecessor = Organisation.objects.create(
        ods_code="PRE",
        ods_name="Predecessor Trust",
        region=region,
        icb=icb,
        successor=successor,
    )
    return predecessor, successor


@pytest.fixture
def vmp():
    vtm = VTM.objects.create(vtm="12345", name="Test VTM")
    return VMP.objects.create(code="12345678", name="Test VMP", vtm=vtm)


@pytest.fixture
def data_status_months():
    months = [
        date(2024, 1, 1),
        date(2024, 2, 1),
        date(2024, 3, 1),
    ]
    for m in months:
        DataStatus.objects.get_or_create(year_month=m)
    return months


@pytest.fixture
def measure(vmp):
    measure = Measure.objects.create(
        name="Test Measure",
        slug="test-measure",
        short_name="TEST",
        quantity_type="ddd",
        status="published",
    )
    MeasureVMP.objects.create(measure=measure, vmp=vmp, type="numerator")
    return measure


def _quantity_payload(vmp, *, ods_codes=None, scope="all"):
    payload = {
        "names": [{"code": vmp.code, "type": "vmp"}],
        "quantity_type": "Defined Daily Dose Quantity",
        "scope": scope,
    }
    if ods_codes is not None:
        payload["ods_codes"] = ods_codes
    return payload


def _post_quantity_data(client, payload):
    return client.post(
        reverse("viewer:get_quantity_data"),
        data=json.dumps(payload),
        content_type="application/json",
    )


def _org_items(items, ods_name):
    return [
        item
        for item in items
        if item.get("organisation__ods_name") == ods_name
    ]


@pytest.mark.django_db
class TestGetQuantityData:
    def test_predecessor_data_aggregated_into_successor(
        self, predecessor_successor_orgs, vmp, data_status_months
    ):
        """
        When requesting data for a successor org, DDDQuantity rows for both
        the successor and its predecessor should be aggregated into a single
        response item under the successor's name.
        """
        predecessor, successor = predecessor_successor_orgs

        DDDQuantity.objects.create(
            vmp=vmp,
            organisation=predecessor,
            data=[10.0, 20.0, 30.0],
        )

        DDDQuantity.objects.create(
            vmp=vmp,
            organisation=successor,
            data=[5.0, 15.0, 25.0],
        )

        response = _post_quantity_data(
            Client(),
            _quantity_payload(vmp, ods_codes=[successor.ods_code]),
        )

        assert response.status_code == 200
        org_items = _org_items(response.json()["items"], successor.ods_name)
        assert len(org_items) == 1, "Should have exactly one item for successor"
        assert org_items[0]["data"] == [15.0, 35.0, 55.0]
        assert org_items[0]["organisation__ods_code"] == successor.ods_code

    def test_filter_by_successor_ods_code_includes_predecessor_rows(
        self, predecessor_successor_orgs, region, icb, vmp, data_status_months
    ):
        """
        Filtering by successor ods_code should include both successor and
        predecessor DDDQuantity rows, and exclude unrelated trusts.
        """
        predecessor, successor = predecessor_successor_orgs
        other = Organisation.objects.create(
            ods_code="OTH",
            ods_name="Other Trust",
            region=region,
            icb=icb,
            successor=None,
        )

        DDDQuantity.objects.create(
            vmp=vmp,
            organisation=predecessor,
            data=[100.0, 0, 0],
        )
        DDDQuantity.objects.create(
            vmp=vmp,
            organisation=successor,
            data=[0, 50.0, 0],
        )
        DDDQuantity.objects.create(
            vmp=vmp,
            organisation=other,
            data=[999.0, 999.0, 999.0],
        )

        response = _post_quantity_data(
            Client(),
            _quantity_payload(vmp, ods_codes=[successor.ods_code]),
        )

        assert response.status_code == 200
        items = response.json()["items"]
        org_items = _org_items(items, successor.ods_name)
        assert len(org_items) == 1
        assert org_items[0]["data"] == [100.0, 50.0, 0.0]
        assert _org_items(items, other.ods_name) == []

    def test_org_without_successor_returns_own_data(
        self, region, icb, vmp, data_status_months
    ):
        """Organisation without successor returns its own data unchanged."""
        org = Organisation.objects.create(
            ods_code="SOL",
            ods_name="Solo Trust",
            region=region,
            icb=icb,
            successor=None,
        )
        DDDQuantity.objects.create(
            vmp=vmp,
            organisation=org,
            data=[1.0, 2.0, 3.0],
        )

        response = _post_quantity_data(
            Client(),
            _quantity_payload(vmp, ods_codes=[org.ods_code]),
        )

        assert response.status_code == 200
        org_items = _org_items(response.json()["items"], org.ods_name)
        assert len(org_items) == 1
        assert org_items[0]["data"] == [1.0, 2.0, 3.0]

    def test_national_scope_returns_aggregated_totals(
        self, predecessor_successor_orgs, region, icb, vmp, data_status_months
    ):
        predecessor, successor = predecessor_successor_orgs
        other = Organisation.objects.create(
            ods_code="OTH",
            ods_name="Other Trust",
            region=region,
            icb=icb,
            successor=None,
        )
        DDDQuantity.objects.create(
            vmp=vmp,
            organisation=predecessor,
            data=[10.0, 0, 0],
        )
        DDDQuantity.objects.create(
            vmp=vmp,
            organisation=successor,
            data=[5.0, 20.0, 0],
        )
        DDDQuantity.objects.create(
            vmp=vmp,
            organisation=other,
            data=[1.0, 2.0, 3.0],
        )

        user = User.objects.create_user(username="analyst", password="pass")
        client = Client()
        client.force_login(user)

        response = _post_quantity_data(
            client,
            _quantity_payload(vmp, scope="national"),
        )

        assert response.status_code == 200
        items = [
            item
            for item in response.json()["items"]
            if item.get("vmp__code") == vmp.code
        ]
        assert len(items) == 1
        assert items[0]["organisation__ods_code"] is None
        assert items[0]["organisation__ods_name"] is None
        assert items[0]["data"] == [16.0, 22.0, 3.0]


@pytest.mark.django_db
class TestGetMeasuresChartData:
    def test_unknown_trust_returns_empty_overlay_for_all_measures(self, measure):
        other_measure = Measure.objects.create(
            name="Other Test Measure",
            slug="other-test-measure",
            short_name="OTHER",
            quantity_type="ddd",
            status="published",
        )

        client = Client()
        response = client.get(
            reverse("viewer:get_measures_chart_data"),
            {"trust": "NOPE"},
        )

        assert response.status_code == 200
        assert response.json()["trust_overlay"] == {
            measure.slug: {"trustData": []},
            other_measure.slug: {"trustData": []},
        }

    def test_predecessor_trust_code_returns_successor_data(
        self,
        predecessor_successor_orgs,
        measure,
        data_status_months,
    ):
        predecessor, successor = predecessor_successor_orgs

        PrecomputedMeasure.objects.create(
            measure=measure,
            organisation=successor,
            month=date(2024, 1, 1),
            quantity=50.0,
            numerator=50.0,
            denominator=None,
        )
        PrecomputedMeasure.objects.create(
            measure=measure,
            organisation=successor,
            month=date(2024, 2, 1),
            quantity=75.0,
            numerator=75.0,
            denominator=None,
        )

        user = User.objects.create_user(
            username="testuser", password="testpass123"
        )
        client = Client()
        client.force_login(user)

        response = client.get(
            reverse("viewer:get_measures_chart_data"),
            {"trust": predecessor.ods_code},
        )

        assert response.status_code == 200
        data = response.json()
        assert "trust_overlay" in data
        assert measure.slug in data["trust_overlay"]

        trust_data = data["trust_overlay"][measure.slug]["trustData"]
        assert len(trust_data) == 2

        values = [v for _, v in trust_data]
        assert 50.0 in values
        assert 75.0 in values

    def test_trust_overlay_fills_missing_months_with_zero(
        self,
        region,
        icb,
        measure,
        data_status_months,
    ):
        trust = Organisation.objects.create(
            ods_code="R0A",
            ods_name="Test Trust",
            region=region,
            icb=icb,
            successor=None,
        )
        months = [
            date(2024, 1, 1),
            date(2024, 2, 1),
            date(2024, 3, 1),
        ]
        for month in months:
            DataStatus.objects.get_or_create(year_month=month)
            PrecomputedPercentile.objects.create(
                measure=measure,
                month=month,
                percentile=50,
                quantity=10.0,
            )

        PrecomputedMeasure.objects.create(
            measure=measure,
            organisation=trust,
            month=date(2024, 1, 1),
            quantity=5.0,
            numerator=5.0,
            denominator=1000.0,
        )
        PrecomputedMeasure.objects.create(
            measure=measure,
            organisation=trust,
            month=date(2024, 3, 1),
            quantity=15.0,
            numerator=15.0,
            denominator=1000.0,
        )

        client = Client()
        response = client.get(
            reverse("viewer:get_measures_chart_data"),
            {"trust": trust.ods_code},
        )

        assert response.status_code == 200
        trust_data = response.json()["trust_overlay"][measure.slug]["trustData"]
        assert trust_data == [
            ["2024-01-01", 5.0],
            ["2024-02-01", 0],
            ["2024-03-01", 15.0],
        ]

    def test_trust_overlay_empty_when_trust_not_in_measure(
        self,
        region,
        icb,
        measure,
    ):
        trust = Organisation.objects.create(
            ods_code="R1A",
            ods_name="No Data Trust",
            region=region,
            icb=icb,
            successor=None,
        )
        for month in [date(2024, 1, 1), date(2024, 2, 1)]:
            DataStatus.objects.get_or_create(year_month=month)
            PrecomputedPercentile.objects.create(
                measure=measure,
                month=month,
                percentile=50,
                quantity=10.0,
            )

        client = Client()
        response = client.get(
            reverse("viewer:get_measures_chart_data"),
            {"trust": trust.ods_code},
        )

        assert response.status_code == 200
        assert response.json()["trust_overlay"][measure.slug]["trustData"] == []


@pytest.mark.django_db
class TestValidateAnalysisParamsVmpCap:

    def _call(self, **params):
        client = Client()
        query = "&".join(
            f"{key}={value}" for key, value in params.items()
        )
        url = reverse("viewer:validate_analysis_params")
        return client.get(f"{url}?{query}" if query else url)

    def _create_vmps(self, n, vtm=None, code_prefix="1000"):
        vmps = []
        for i in range(n):
            vmps.append(
                VMP.objects.create(
                    code=f"{code_prefix}{i:04d}",
                    name=f"VMP {i}",
                    vtm=vtm,
                )
            )
        return vmps

    def test_accepts_selection_exactly_at_cap(self):
        vtm = VTM.objects.create(vtm="100", name="Cap VTM")
        self._create_vmps(MAX_ANALYSIS_VMP_COUNT, vtm=vtm, code_prefix="2000")

        response = self._call(vtms=vtm.vtm)

        assert response.status_code == 200
        data = response.json()
        assert data["errors"] == []
        assert data["vmp_count"] == MAX_ANALYSIS_VMP_COUNT
        assert len(data["valid_products"]) == 1

    def test_rejects_selection_one_over_cap(self):
        vtm = VTM.objects.create(vtm="101", name="Over-cap VTM")
        self._create_vmps(MAX_ANALYSIS_VMP_COUNT + 1, vtm=vtm, code_prefix="3000")

        response = self._call(vtms=vtm.vtm)

        assert response.status_code == 200
        data = response.json()
        assert data["vmp_count"] == MAX_ANALYSIS_VMP_COUNT + 1
        assert any(
            "maximum of" in err.lower() and "unique products" in err.lower()
            for err in data["errors"]
        ), f"Expected cap error in {data['errors']!r}"

    def test_counts_union_not_sum_across_overlapping_selections(self):
        """Two ingredients with overlapping VMPs count as a union, not a sum."""
        ingredient_a = Ingredient.objects.create(code="900000001", name="Ingredient A")
        ingredient_b = Ingredient.objects.create(code="900000002", name="Ingredient B")

        overlap = self._create_vmps(3, code_prefix="4000")
        only_a = self._create_vmps(1, code_prefix="4001")
        only_b = self._create_vmps(1, code_prefix="4002")

        for vmp in overlap + only_a:
            vmp.ingredients.add(ingredient_a)
        for vmp in overlap + only_b:
            vmp.ingredients.add(ingredient_b)

        response = self._call(ingredients=f"{ingredient_a.code},{ingredient_b.code}")

        assert response.status_code == 200
        data = response.json()
        assert data["errors"] == []
        assert data["vmp_count"] == 5
