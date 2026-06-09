from datetime import date

import pytest
from django.core.management import call_command

from viewer.models import (
    DataStatus,
    DDDQuantity,
    ICB,
    Measure,
    MeasureVMP,
    Organisation,
    PrecomputedMeasure,
    Region,
    TrustAdmission,
    VMP,
    VTM,
)


@pytest.fixture
def region():
    return Region.objects.create(name="Test Region", code="TR")


@pytest.fixture
def icb(region):
    return ICB.objects.create(code="QXX", name="Test ICB", region=region)


@pytest.fixture
def trust(region, icb):
    return Organisation.objects.create(
        ods_code="R0A",
        ods_name="Test Trust",
        region=region,
        icb=icb,
    )


@pytest.fixture
def trust_without_admissions(region, icb):
    return Organisation.objects.create(
        ods_code="RR8",
        ods_name="No Admissions Trust",
        region=region,
        icb=icb,
    )


@pytest.fixture
def vmp():
    vtm = VTM.objects.create(vtm="12345", name="Test VTM")
    return VMP.objects.create(code="12345678", name="Test VMP", vtm=vtm)


@pytest.fixture
def months():
    m1 = date(2024, 1, 1)
    m2 = date(2024, 2, 1)
    for m in (m1, m2):
        DataStatus.objects.get_or_create(year_month=m)
    return [m1, m2]


@pytest.fixture
def admissions_measure(vmp):
    measure = Measure.objects.create(
        name="IV per 1000 admissions",
        slug="iv-per-1000",
        short_name="IV/1000",
        quantity_type="ddd",
        denominator_type="1000_admissions",
        why_it_matters="test",
        how_is_it_calculated="test",
        status="in_development",
    )
    MeasureVMP.objects.create(measure=measure, vmp=vmp, type="numerator")
    return measure


@pytest.mark.django_db
def test_compute_measures_per_1000_admissions(
    admissions_measure, trust, trust_without_admissions, vmp, months
):
    TrustAdmission.objects.create(organisation=trust, period=months[0], count=1000)
    TrustAdmission.objects.create(organisation=trust, period=months[1], count=2000)

    DDDQuantity.objects.create(
        vmp=vmp,
        organisation=trust,
        data=[50.0, 100.0],
    )
    DDDQuantity.objects.create(
        vmp=vmp,
        organisation=trust_without_admissions,
        data=[25.0, 25.0],
    )

    call_command("compute_measures", admissions_measure.slug)

    included = PrecomputedMeasure.objects.filter(measure=admissions_measure)
    assert included.count() == 2
    assert not included.filter(organisation=trust_without_admissions).exists()

    jan = included.get(organisation=trust, month=months[0])
    assert jan.numerator == 50.0
    assert jan.denominator == 1000.0
    assert jan.quantity == pytest.approx(50.0)

    feb = included.get(organisation=trust, month=months[1])
    assert feb.quantity == pytest.approx(50.0)
