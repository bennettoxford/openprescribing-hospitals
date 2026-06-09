from collections.abc import Callable
from collections import defaultdict
from dataclasses import dataclass
from datetime import date

from django.apps import apps

ADMISSIONS_DENOMINATOR_TYPE = '1000_admissions'

@dataclass(frozen=True)
class Denominator:
    key: str
    label: str
    scale: float
    chart_kind: str
    get_values_by_org_month: Callable[[], dict[int, dict[date, float]]]


def get_admissions_by_org_month() -> dict[int, dict[date, float]]:
    """Return {organisation_id: {period_date: admission_count}}."""

    TrustAdmission = apps.get_model("viewer", "TrustAdmission")
    admissions = defaultdict(dict)
    for row in TrustAdmission.objects.values('organisation_id', 'period', 'count'):
        admissions[row['organisation_id']][row['period']] = float(row['count'])
    return admissions


EXTERNAL_DENOMINATORS = {
    ADMISSIONS_DENOMINATOR_TYPE: Denominator(
        key=ADMISSIONS_DENOMINATOR_TYPE,
        label='Per 1000 admissions',
        scale=1000,
        chart_kind='per_1000_admissions',
        get_values_by_org_month=get_admissions_by_org_month,
    ),
}
EXTERNAL_DENOMINATOR_CHOICES = [
    (denominator.key, denominator.label)
    for denominator in EXTERNAL_DENOMINATORS.values()
]
EXTERNAL_DENOMINATOR_TYPES = tuple(EXTERNAL_DENOMINATORS)


def get_external_denominator(measure) -> Denominator | None:
    denominator_type = getattr(measure, 'denominator_type', None)
    if not denominator_type:
        return None
    return EXTERNAL_DENOMINATORS.get(denominator_type)


def measure_uses_external_denominator(measure) -> bool:
    return get_external_denominator(measure) is not None


def measure_uses_admissions_denominator(measure) -> bool:
    return measure.denominator_type == ADMISSIONS_DENOMINATOR_TYPE


def measure_has_product_denominator(measure) -> bool:
    annotated = getattr(measure, 'has_product_denominator', None)
    if annotated is not None:
        return bool(annotated)

    from viewer.models import MeasureVMP

    return MeasureVMP.objects.filter(measure=measure, type='denominator').exists()


def get_external_denominator_values_by_org_month(measure) -> dict[int, dict[date, float]]:
    denominator = get_external_denominator(measure)
    if denominator is None:
        return {}
    return denominator.get_values_by_org_month()


def get_measure_chart_kind(measure, has_product_denominator: bool | None = None) -> str:
    external_denominator = get_external_denominator(measure)
    if external_denominator is not None:
        return external_denominator.chart_kind
    if has_product_denominator is None:
        has_product_denominator = measure_has_product_denominator(measure)
    if has_product_denominator:
        return 'percentage'
    return 'absolute'


def measure_has_rate_denominator(measure) -> bool:
    return measure_has_product_denominator(measure) or measure_uses_external_denominator(measure)


def compute_rate_from_totals(numerator: float, denominator: float, measure) -> float:
    if denominator <= 0:
        return 0.0
    external_denominator = get_external_denominator(measure)
    scale = external_denominator.scale if external_denominator is not None else 100
    return (numerator / denominator) * scale
