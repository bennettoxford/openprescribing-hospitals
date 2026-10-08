import pytest
from datetime import date

from django.utils.text import slugify

from viewer.models import (
    Region,
    ICB,
    Organisation,
    Measure,
    MeasureGroup,
    MeasureVMP,
    PrecomputedMeasure,
    VMP,
    VTM,
    DataStatus,
)
from viewer.views.measures import (
    normalise_trust_code,
    build_measure_org_data,
    build_trust_chart_data,
    series_dict_to_chart_points,
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


class TestTrustSeriesGapFill:
    def test_series_dict_to_chart_points_fills_missing_months_with_zero(self):
        months = [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)]
        values = {date(2024, 1, 1): 10.0, date(2024, 3, 1): 30.0}
        assert series_dict_to_chart_points(months, values) == [
            ["2024-01-01", 10.0],
            ["2024-02-01", 0],
            ["2024-03-01", 30.0],
        ]

    def test_build_trust_chart_data_fills_trust_overlay(self):
        measure = type("Measure", (), {"id": 42})()
        bulk_percentiles = {
            measure.id: {
                date(2024, 1, 1): {50: 10.0},
                date(2024, 2, 1): {50: 20.0},
                date(2024, 3, 1): {50: 30.0},
            }
        }
        overlay = {date(2024, 1, 1): 1.0, date(2024, 3, 1): 3.0}
        chart = build_trust_chart_data(measure, bulk_percentiles, overlay_series=overlay)
        assert chart["trustData"] == [
            ["2024-01-01", 1.0],
            ["2024-02-01", 0],
            ["2024-03-01", 3.0],
        ]


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


def _published_measure(slug, name, group=None):
    measure_group = None
    if isinstance(group, str):
        measure_group, _created = MeasureGroup.objects.get_or_create(
            slug=slugify(group),
            defaults={'name': group},
        )
    elif group is not None:
        measure_group = group
    return Measure.objects.create(
        name=name,
        slug=slug,
        short_name=name,
        description=f'{name} description',
        why_it_matters='Because',
        how_is_it_calculated='How',
        quantity_type='ddd',
        status='published',
        measure_group=measure_group,
    )


@pytest.mark.django_db
class TestMeasureGroups:
    def test_group_page_lists_only_group_members(self, client):
        _published_measure('atropine-pfs', 'Atropine PFS', 'Pre-filled syringes')
        _published_measure('adrenaline-pfs', 'Adrenaline PFS', 'Pre-filled syringes')
        _published_measure('other-measure', 'Other measure')

        response = client.get('/measures/group/pre-filled-syringes/')
        assert response.status_code == 200
        content = response.content.decode()
        assert 'atropine-pfs' in content
        assert 'adrenaline-pfs' in content
        assert 'other-measure' not in content
        assert 'Pre-filled syringes' in content
        assert 'These measures belong to the Pre-filled syringes group.' in content
        assert 'collapseGroups="false"' in content
        assert 'aria-label="Breadcrumb"' in content
        assert 'Back to all measures' not in content

    def test_group_page_shows_the_group_description(self, client):
        group = MeasureGroup.objects.create(
            name='Low value prescribing',
            slug='low-value-prescribing',
            description='These items provide low value when prescribed.',
        )
        _published_measure('aliskiren', 'Aliskiren', group)

        response = client.get('/measures/group/low-value-prescribing/')
        assert response.status_code == 200
        content = response.content.decode()
        membership = 'These measures belong to the Low value prescribing group.'
        description = 'These items provide low value when prescribed.'
        assert membership in content
        assert description in content
        assert content.index(membership) < content.index(description)

    def test_index_keeps_grouped_measures_in_page_data(self, client):
        _published_measure('atropine-pfs', 'Atropine PFS', 'Pre-filled syringes')
        _published_measure('other-measure', 'Other measure')

        response = client.get('/measures/')
        assert response.status_code == 200
        content = response.content.decode()
        assert 'atropine-pfs' in content
        assert 'other-measure' in content
        assert 'Pre-filled syringes' in content
        assert 'collapseGroups="true"' in content
        assert 'aria-label="Breadcrumb"' not in content

    def test_unknown_group_returns_404(self, client):
        _published_measure('other-measure', 'Other measure')
        response = client.get('/measures/group/missing-group/')
        assert response.status_code == 404

    def test_measure_page_lists_the_group(self, client):
        _published_measure('atropine-pfs', 'Atropine PFS', 'Pre-filled syringes')
        _published_measure('adrenaline-pfs', 'Adrenaline PFS', 'Pre-filled syringes')

        response = client.get('/measures/atropine-pfs/')
        assert response.status_code == 200
        content = response.content.decode()
        assert 'data-measure-group="pre-filled-syringes"' in content
        assert '>Group:</span>' in content
        assert 'Pre-filled syringes' in content
        assert 'Adrenaline PFS' not in content
        assert 'Show the other measures in this group' not in content
        assert '/measures/group/pre-filled-syringes/' in content

    def test_ungrouped_measure_page_has_no_group_panel(self, client):
        _published_measure('other-measure', 'Other measure')
        response = client.get('/measures/other-measure/')
        assert response.status_code == 200
        assert 'data-measure-group=' not in response.content.decode()

