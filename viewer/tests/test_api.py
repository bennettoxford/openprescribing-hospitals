import json
import pytest
from datetime import date
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
)


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

        client = Client()
        payload = {
            "names": [{"code": vmp.code, "type": "vmp"}],
            "quantity_type": "Defined Daily Dose Quantity",
            "ods_names": [successor.ods_name],
        }
        response = client.post(
            reverse("viewer:get_quantity_data"),
            data=json.dumps(payload),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = response.json()
        items = data["items"]

        org_items = [
            i
            for i in items
            if i.get("organisation__ods_name") == successor.ods_name
        ]
        assert len(org_items) == 1, "Should have exactly one item for successor"
        org_item = org_items[0]

        expected_data = [15.0, 35.0, 55.0]
        assert org_item["data"] == expected_data

    def test_filter_by_successor_includes_predecessor_rows(
        self, predecessor_successor_orgs, vmp, data_status_months
    ):
        """
        Filtering by successor ods_name should include both successor and
        predecessor DDDQuantity rows.
        """
        predecessor, successor = predecessor_successor_orgs

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

        client = Client()
        payload = {
            "names": [{"code": vmp.code, "type": "vmp"}],
            "quantity_type": "Defined Daily Dose Quantity",
            "ods_names": [successor.ods_name],
        }
        response = client.post(
            reverse("viewer:get_quantity_data"),
            data=json.dumps(payload),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = response.json()
        org_items = [
            i
            for i in data["items"]
            if i.get("organisation__ods_name") == successor.ods_name
        ]
        assert len(org_items) == 1
        assert org_items[0]["data"] == [100.0, 50.0, 0.0]

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

        client = Client()
        payload = {
            "names": [{"code": vmp.code, "type": "vmp"}],
            "quantity_type": "Defined Daily Dose Quantity",
            "ods_names": [org.ods_name],
        }
        response = client.post(
            reverse("viewer:get_quantity_data"),
            data=json.dumps(payload),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = response.json()
        org_items = [
            i
            for i in data["items"]
            if i.get("organisation__ods_name") == org.ods_name
        ]
        assert len(org_items) == 1
        assert org_items[0]["data"] == [1.0, 2.0, 3.0]
