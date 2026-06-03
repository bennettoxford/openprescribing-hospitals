import pytest
from datetime import date

from viewer.models import (
    Region,
    ICB,
    Organisation,
    Measure,
    MeasureVMP,
    PrecomputedMeasure,
    VMP,
    VTM,
    DataStatus,
)
from viewer.views.measures import (
    normalise_trust_code,
    build_measure_org_data,
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


@pytest.fixture
def data_status_months():
    months = [date(2024, 1, 1), date(2024, 2, 1)]
    for m in months:
        DataStatus.objects.get_or_create(year_month=m)
    return months


@pytest.mark.django_db
class TestNormaliseTrustCode:
    def test_predecessor_code_returns_successor_code(
        self, predecessor_successor_orgs
    ):
        predecessor, successor = predecessor_successor_orgs
        assert normalise_trust_code(predecessor.ods_code) == successor.ods_code

    def test_successor_code_returns_self(self, predecessor_successor_orgs):
        _, successor = predecessor_successor_orgs
        assert normalise_trust_code(successor.ods_code) == successor.ods_code

    def test_org_without_successor_returns_own_code(self, region, icb):
        org = Organisation.objects.create(
            ods_code="SOLO",
            ods_name="Solo Trust",
            region=region,
            icb=icb,
            successor=None,
        )
        assert normalise_trust_code(org.ods_code) == org.ods_code

    def test_unknown_code_returns_none(self):
        assert normalise_trust_code("UNKNOWN") is None

    def test_empty_code_returns_none(self):
        assert normalise_trust_code("") is None
        assert normalise_trust_code(None) is None


@pytest.mark.django_db
class TestBuildMeasureOrgData:
    def test_returns_successors_only(
        self, predecessor_successor_orgs, measure, data_status_months
    ):
        predecessor, successor = predecessor_successor_orgs

        PrecomputedMeasure.objects.create(
            measure=measure,
            organisation=successor,
            month=date(2024, 1, 1),
            quantity=100.0,
            numerator=100.0,
            denominator=None,
        )

        org_measures = PrecomputedMeasure.objects.filter(measure=measure)
        shared_org_data = {"org_codes": {successor.ods_name: successor.ods_code}}
        result = build_measure_org_data(org_measures, shared_org_data)

        org_names = [o["name"] for o in result["organisations"]]
        assert successor.ods_name in org_names
        assert predecessor.ods_name not in org_names

