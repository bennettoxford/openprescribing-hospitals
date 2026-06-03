import pytest

from viewer.measure_denominators import (
    EXTERNAL_DENOMINATOR_TYPES,
    compute_rate_from_totals,
    get_external_denominator,
    get_measure_chart_kind,
    measure_has_rate_denominator,
    measure_uses_external_denominator,
    measure_uses_admissions_denominator,
)
from viewer.models import Measure, VMP, VTM


@pytest.fixture
def vmp():
    vtm = VTM.objects.create(vtm="12345", name="Test VTM")
    return VMP.objects.create(code="12345678", name="Test VMP", vtm=vtm)


@pytest.mark.django_db
def test_admissions_rate_calculation(vmp):
    measure = Measure.objects.create(
        name="Admissions measure",
        slug="adm-measure",
        short_name="ADM",
        quantity_type="ddd",
        denominator_type="1000_admissions",
        why_it_matters="x",
        how_is_it_calculated="x",
    )
    assert measure_uses_admissions_denominator(measure)
    assert measure_uses_external_denominator(measure)
    assert measure_has_rate_denominator(measure)
    assert get_external_denominator(measure).scale == 1000
    assert get_measure_chart_kind(measure) == "per_1000_admissions"
    assert compute_rate_from_totals(50, 1000, measure) == pytest.approx(50.0)
    assert "1000_admissions" in EXTERNAL_DENOMINATOR_TYPES

