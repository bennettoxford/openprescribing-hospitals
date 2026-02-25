import hashlib
import json
from collections import defaultdict
from datetime import datetime, timedelta
from markdown2 import Markdown
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.cache import cache
from django.views.generic import TemplateView
from django.core.serializers.json import DjangoJSONEncoder
from django.shortcuts import redirect
from django.urls import reverse, reverse_lazy
from django.utils.text import slugify
from django.db.models import Count, Exists, OuterRef

from ..mixins import MaintenanceModeMixin
from ..models import (
    Measure,
    MeasureVMP,
    MeasureTag,
    PrecomputedMeasure,
    PrecomputedMeasureAggregated,
    PrecomputedPercentile,
    Organisation,
    MeasureAnnotation,
    Region,
    ICB,
)
from ..utils import get_organisation_data


PERCENTILE_LEVELS = [5, 15, 25, 35, 45, 50, 55, 65, 75, 85, 95]
MEASURES_LIST_CHART_CACHE_TIMEOUT = 86400
MEASURES_LIST_CHART_CACHE_VERSION_KEY = 'measures_list_chart_version'
MEASURE_ITEM_PRECOMPUTED_CACHE_TIMEOUT = 86400
MEASURE_ITEM_PRECOMPUTED_CACHE_KEY_PREFIX = 'measure_item_precomputed:'


def invalidate_measures_list_chart_cache():
    """Invalidate the measures list chart cache by bumping the version. Call after computing measures."""
    version = cache.get(MEASURES_LIST_CHART_CACHE_VERSION_KEY, 0)
    cache.set(MEASURES_LIST_CHART_CACHE_VERSION_KEY, version + 1, timeout=None)


def invalidate_measure_item_cache(measure_slug):
    """Invalidate the measure item precomputed data cache. Call after computing a measure."""
    cache.delete(f'{MEASURE_ITEM_PRECOMPUTED_CACHE_KEY_PREFIX}{measure_slug}')

def _is_measure_new(measure):
    """True if measure was first published within the last 30 days."""
    if not measure.first_published:
        return False
    one_month_ago = datetime.now().date() - timedelta(days=30)
    return measure.first_published >= one_month_ago

def get_bulk_percentiles_for_measures(measures):
    """Fetch all percentiles for a set of measures."""
    measure_ids = [m.id for m in measures]
    percentiles = PrecomputedPercentile.objects.filter(
        measure_id__in=measure_ids,
        percentile__in=PERCENTILE_LEVELS
    ).values('measure_id', 'month', 'percentile', 'quantity').order_by('measure_id', 'month', 'percentile')

    grouped_data = defaultdict(lambda: defaultdict(dict))
    for p in percentiles:
        grouped_data[p['measure_id']][p['month']][p['percentile']] = p['quantity']

    return grouped_data


def expand_trust_codes(trust_code):
    """
    Expand a trust ODS code to include all predecessor codes.
    Returns a list of ODS codes to query.
    """
    if not trust_code:
        return []

    try:
        org = Organisation.objects.get(ods_code=trust_code)
        all_codes = [org.ods_code] + org.get_all_predecessor_codes()
        return all_codes
    except Organisation.DoesNotExist:
        return []


def _compute_measure_value_from_rows(measure, rows):
    """Compute chart value from PrecomputedMeasure rows. Reused by get_bulk_trust_series_for_measures."""
    if measure.has_denominators:
        total_numerator = sum(row['numerator'] or 0 for row in rows)
        total_denominator = sum(row['denominator'] or 0 for row in rows)
        if total_denominator > 0:
            value = (total_numerator / total_denominator) * 100
        else:
            value = 0
        return max(0, min(100, value))
    return max(0, sum(row['quantity'] or 0 for row in rows))


def get_bulk_trust_series_for_measures(measures, trust_codes=None, per_org=False):
    """
    Fetch trust series for measures.

    - trust_codes provided, per_org=False: aggregated series for selected trust(s)
      (merges predecessors). Returns { measure_id: { month: value } }.
    - trust_codes=None, per_org=True: per-org series for all trusts.
      Returns { measure_id: { org_name: [[date_iso, value], ...] } }.
    """
    if not measures:
        return {}
    if not per_org and not trust_codes:
        return {}

    measure_ids = [m.id for m in measures]
    measure_by_id = {m.id: m for m in measures}

    qs = PrecomputedMeasure.objects.filter(measure_id__in=measure_ids)
    if trust_codes:
        qs = qs.filter(organisation__ods_code__in=trust_codes)
    qs = qs.values(
        'measure_id', 'organisation__ods_name', 'month', 'quantity', 'numerator', 'denominator'
    ).order_by('measure_id', 'organisation__ods_name', 'month')

    if per_org:
        grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for row in qs:
            org_name = row['organisation__ods_name']
            if org_name:
                grouped[row['measure_id']][org_name][row['month']].append(row)
        result = defaultdict(dict)
        for measure_id, orgs_data in grouped.items():
            measure = measure_by_id.get(measure_id)
            if not measure:
                continue
            for org_name, months_data in orgs_data.items():
                series = [
                    [month.isoformat(), _compute_measure_value_from_rows(measure, rows)]
                    for month, rows in sorted(months_data.items())
                ]
                if series:
                    result[measure_id][org_name] = series
        return dict(result)
    else:
        grouped_data = defaultdict(lambda: defaultdict(list))
        for row in qs:
            grouped_data[row['measure_id']][row['month']].append(row)
        aggregated_data = defaultdict(dict)
        for measure_id, months_data in grouped_data.items():
            measure = measure_by_id.get(measure_id)
            if not measure:
                continue
            for month, rows in months_data.items():
                aggregated_data[measure_id][month] = _compute_measure_value_from_rows(measure, rows)
        return aggregated_data


def get_bulk_regions_and_national_series_for_measures(measures):
    """
    Fetch region and national series data for a set of measures.
    """
    measure_ids = [m.id for m in measures]
    rows = PrecomputedMeasureAggregated.objects.filter(
        measure_id__in=measure_ids,
        category__in=['region', 'national']
    ).values('measure_id', 'category', 'label', 'month', 'quantity').order_by('measure_id', 'category', 'label', 'month')

    regions_data = defaultdict(lambda: defaultdict(dict))
    national_data = defaultdict(dict)
    for row in rows:
        if row['category'] == 'region':
            regions_data[row['measure_id']][row['label']][row['month']] = row['quantity']
        else:
            national_data[row['measure_id']][row['month']] = row['quantity']
    return regions_data, national_data


def get_region_list():
    """Get list of all regions with code and name."""
    regions = Region.objects.all().order_by('name')
    return [{'code': r.code, 'name': r.name} for r in regions]


def build_national_chart_data(measure, bulk_national):
    """Return chart dict for national-mode chart."""
    data = bulk_national.get(measure.id, {})
    if not data:
        return None
    return {'data': [[m.isoformat(), v] for m, v in sorted(data.items())]}


def build_region_chart_data(measure, bulk_all_regions):
    """Return chart dict for region-mode chart"""
    regions_data = bulk_all_regions.get(measure.id, {})
    if not regions_data:
        return None
    return {
        'regions': {
            name: {'data': [[m.isoformat(), v] for m, v in sorted(months.items())]}
            for name, months in regions_data.items()
        },
    }


def build_trust_chart_data(measure, bulk_percentiles, overlay_series=None):
    """Return chart dict for trust-mode (percentile) chart."""
    measure_data = bulk_percentiles.get(measure.id, {})
    if not measure_data:
        return None

    percentiles = {}
    for p in PERCENTILE_LEVELS:
        values = [
            [month.isoformat(), measure_data[month][p]]
            for month in sorted(measure_data)
            if p in measure_data[month]
        ]
        if values:
            percentiles[p] = values

    chart_data = {'percentiles': percentiles}

    if overlay_series:
        chart_data['trustData'] = [[m.isoformat(), v] for m, v in sorted(overlay_series.items())]

    return chart_data


def build_measure_org_data(org_measures, shared_org_data, include_region_icb=False):
    """
    Build org data for measure views.

    When include_region_icb=True, adds region/icb per org and regions_hierarchy for RegionIcbFilter.
    """
    measure_orgs = set(
        org_measures.values_list('organisation__ods_code', flat=True).distinct()
    )
    if include_region_icb:
        current_orgs = Organisation.objects.filter(
            successor__isnull=True
        ).select_related('region', 'icb').values(
            'ods_code', 'ods_name',
            'region__name', 'region__code',
            'icb__name', 'icb__code',
        ).order_by('ods_name')
    else:
        current_orgs = Organisation.objects.filter(
            successor__isnull=True
        ).values('ods_code', 'ods_name').order_by('ods_name')

    flat_data = {}
    predecessor_map = shared_org_data['predecessor_map']
    org_codes = shared_org_data['org_codes']
    regions_with_icbs = defaultdict(lambda: {'icbs': set(), 'region_code': None}) if include_region_icb else None

    for org in current_orgs:
        org_name = org['ods_name']
        is_available = org['ods_code'] in measure_orgs
        entry = {'available': is_available, 'data': []}
        if include_region_icb:
            region = org.get('region__name')
            region_code = org.get('region__code')
            icb = org.get('icb__name')
            icb_code = org.get('icb__code')
            entry['region'] = region
            entry['icb'] = icb
            if region and icb:
                regions_with_icbs[region]['region_code'] = region_code
                regions_with_icbs[region]['icbs'].add((icb, icb_code or ''))
        flat_data[org_name] = entry

    for successor, predecessors in predecessor_map.items():
        if successor in flat_data:
            pred_entry = {
                'available': flat_data[successor]['available'],
                'data': [],
            }
            if include_region_icb:
                pred_entry['region'] = flat_data[successor].get('region')
                pred_entry['icb'] = flat_data[successor].get('icb')
            for predecessor in predecessors:
                flat_data[predecessor] = dict(pred_entry)

    for row in org_measures.values(
        'organisation__ods_code', 'organisation__ods_name',
        'month', 'quantity', 'numerator', 'denominator',
    ):
        org_name = row['organisation__ods_name']
        if org_name in flat_data:
            flat_data[org_name]['data'].append({
                'month': row['month'],
                'quantity': row['quantity'],
                'numerator': row['numerator'],
                'denominator': row['denominator'],
            })

    for successor, predecessors in predecessor_map.items():
        if successor in flat_data and flat_data[successor]['data']:
            succ_data = flat_data[successor]['data']
            for predecessor in predecessors:
                if predecessor in flat_data:
                    flat_data[predecessor]['data'] = list(succ_data)

    orgs_with_data = {
        name for name, info in flat_data.items()
        if info.get('available') and info.get('data')
    }
    all_predecessors = set()
    for preds in predecessor_map.values():
        all_predecessors.update(preds)

    current_org_names = [
        name for name in flat_data
        if name in predecessor_map or name not in all_predecessors
    ]
    current_org_names.sort(key=lambda n: n.lower())

    processed = set()

    def build_org_entry(org_name):
        if org_name not in flat_data or org_name in processed:
            return None
        processed.add(org_name)
        entry = flat_data[org_name]
        org_entry = {
            'name': org_name,
            'ods_code': org_codes.get(org_name),
            'data': entry.get('data', []),
            'region': entry.get('region'),
            'icb': entry.get('icb'),
            'available': entry.get('available', False),
            'predecessors': [],
        }
        for pred in predecessor_map.get(org_name, []):
            pred_entry = build_org_entry(pred)
            if pred_entry:
                org_entry['predecessors'].append(pred_entry)
        return org_entry

    organisations = []
    for org_name in current_org_names:
        if org_name not in processed:
            org_entry = build_org_entry(org_name)
            if org_entry:
                organisations.append(org_entry)

    result = {
        'organisations': organisations,
        'org_codes': org_codes,
        'predecessor_map': predecessor_map,
        'available_count': len(orgs_with_data),
    }
    if include_region_icb and regions_with_icbs:
        result['regions_hierarchy'] = [
            {
                'region': region,
                'region_code': data['region_code'],
                'icbs': sorted(
                    [{'name': n, 'code': c} for n, c in data['icbs']],
                    key=lambda x: x['name'],
                ),
            }
            for region, data in sorted(regions_with_icbs.items())
        ]
    return result


def _serialize_measures(measures, detail_url_name, prefetched):
    nat = prefetched.get('national', {}) or {}
    reg = prefetched.get('region', {}) or {}
    trust = prefetched.get('trust_percentiles', {}) or {}
    detail_base_url = (
        reverse('viewer:measures_preview_list')
        if 'preview' in detail_url_name
        else reverse('viewer:measures_list')
    )
    serialized = []
    for measure in measures:
        tags = list(measure.tags.all().order_by('name'))
        tag_slugs = ','.join(slugify(t.name) for t in tags) if tags else ''
        has_chart_data = bool(nat.get(measure.slug) or reg.get(measure.slug) or trust.get(measure.slug))
        serialized.append({
            'slug': measure.slug,
            'short_name': (measure.short_name or measure.name) or '',
            'description': (measure.description or ''),
            'lower_is_better': measure.lower_is_better,
            'tags': [
                {'name': t.name, 'slug': slugify(t.name), 'colour': t.colour or '#6b7280', 'description': t.description or ''}
                for t in tags
            ],
            'tag_slugs': tag_slugs,
            'is_new': getattr(measure, 'is_new', False),
            'has_denominators': getattr(measure, 'has_denominators', False),
            'quantity_type': measure.quantity_type or '',
            'first_published': measure.first_published.isoformat() if measure.first_published else None,
            'detail_url_name': detail_url_name,
            'detail_base_url': detail_base_url,
            'has_chart_data': has_chart_data,
        })
    return json.dumps(serialized, cls=DjangoJSONEncoder)


class MeasuresListView(MaintenanceModeMixin, TemplateView):
    template_name = "measures_list.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        is_authenticated = self.request.user.is_authenticated
        preview_mode = self.kwargs.get('preview_mode', False)

        if is_authenticated:
            selected_mode = (self.request.GET.get('mode') or 'trust').strip()
            if selected_mode not in ('national', 'region', 'trust'):
                selected_mode = 'trust'
            selected_trust_code = (self.request.GET.get('trust') or '').strip() if selected_mode == 'trust' else ''
            selected_region = (self.request.GET.get('region') or '').strip() if selected_mode == 'region' else ''
            sort = (self.request.GET.get('sort') or 'name').strip()
            denom_exists = MeasureVMP.objects.filter(measure=OuterRef('pk'), type='denominator')
            if preview_mode:
                preview_measures = Measure.objects.filter(status='preview').annotate(
                    has_denominators=Exists(denom_exists)
                ).prefetch_related('tags').order_by('name')
                in_development_measures = Measure.objects.filter(status='in_development').annotate(
                    has_denominators=Exists(denom_exists)
                ).prefetch_related('tags').order_by('name')
                measures = list(preview_measures) + list(in_development_measures)
            else:
                measures = Measure.objects.filter(status='published').annotate(
                    has_denominators=Exists(denom_exists)
                ).prefetch_related('tags').order_by('name')
                preview_measures = []
                in_development_measures = []
        else:
            sort = 'name'
            if preview_mode:
                preview_measures = Measure.objects.filter(status='preview').prefetch_related('tags').order_by('name')
                in_development_measures = []
                measures = list(preview_measures)
            else:
                measures = Measure.objects.filter(status='published').prefetch_related('tags').order_by('name')
                preview_measures = []
                in_development_measures = []

        if preview_mode:
            # Only show tags used by preview and in_development measures
            measure_tags = list(
                MeasureTag.objects.filter(
                    measures__status__in=['preview', 'in_development']
                ).distinct().order_by('name')
            )
        else:
            # Only show tags used by published measures
            measure_tags = list(
                MeasureTag.objects.filter(measures__status='published')
                .distinct().order_by('name')
            )
        tags_param = (self.request.GET.get('tags') or '').strip() if is_authenticated else ''
        selected_tag = ','.join(s.strip() for s in tags_param.split(',') if s.strip()) if tags_param else ''

        markdowner = Markdown()
        for measure in measures:
            measure.why_it_matters = markdowner.convert(measure.why_it_matters)
            if is_authenticated:
                measure.tag_slugs = ','.join(slugify(t.name) for t in measure.tags.all())
            measure.is_new = _is_measure_new(measure)

        if is_authenticated:
            selected_code = {'trust': selected_trust_code, 'region': selected_region}.get(selected_mode, '')
            org_data = get_organisation_data()
            region_list = get_region_list()
            context.update({
                "tags_json": json.dumps(
                    [{"id": t.id, "name": t.name, "slug": slugify(t.name), "colour": t.colour or "#6b7280"}
                     for t in measure_tags],
                    cls=DjangoJSONEncoder,
                ),
                "selected_mode": selected_mode,
                "selected_sort": sort,
                "selected_code": selected_code,
                "selected_tag": selected_tag,
                "selected_trust_code": selected_trust_code,
                "selected_trust_codes_param": selected_trust_code if selected_mode == 'trust' else "",
                "org_data_json": json.dumps(org_data, cls=DjangoJSONEncoder),
                "region_data_json": json.dumps(region_list, cls=DjangoJSONEncoder),
            })

            if not measures:
                prefetched = {'national': {}, 'region': {}, 'trust_percentiles': {}, 'modes_by_slug': {}}
            else:
                slugs_part = ",".join(sorted(m.slug for m in measures))
                slugs_hash = hashlib.sha256(slugs_part.encode()).hexdigest()[:16]
                cache_version = cache.get(MEASURES_LIST_CHART_CACHE_VERSION_KEY, 0)
                cache_key = "measures_list_chart_data:{}:{}:{}".format(
                    "preview" if preview_mode else "published",
                    slugs_hash,
                    cache_version,
                )
                cached_chart = cache.get(cache_key)
                if cached_chart is not None:
                    national_data, region_data, trust_percentiles_data = cached_chart
                else:
                    bulk_all_regions, bulk_national = get_bulk_regions_and_national_series_for_measures(measures)
                    bulk_percentiles = get_bulk_percentiles_for_measures(measures)
                    trust_counts = dict(
                        PrecomputedMeasure.objects.filter(measure_id__in=[m.id for m in measures])
                        .values('measure_id')
                        .annotate(count=Count('organisation_id', distinct=True))
                        .values_list('measure_id', 'count')
                    )
                    national_data = {}
                    region_data = {}
                    trust_percentiles_data = {}
                    measures_with_few_trusts = [m for m in measures if trust_counts.get(m.id, 0) < 30]
                    bulk_trust_series_per_org = (
                        get_bulk_trust_series_for_measures(measures_with_few_trusts, per_org=True)
                        if measures_with_few_trusts else {}
                    )
                    for measure in measures:
                        national_data[measure.slug] = build_national_chart_data(measure, bulk_national)
                        region_data[measure.slug] = build_region_chart_data(measure, bulk_all_regions)
                        chart_data = build_trust_chart_data(measure, bulk_percentiles)
                        trust_count = trust_counts.get(measure.id, 0)
                        trust_series = bulk_trust_series_per_org.get(measure.id, {}) if trust_count < 30 else {}
                        if chart_data:
                            chart_data['trust_count'] = trust_count
                            if trust_series:
                                chart_data['trustSeries'] = trust_series
                            trust_percentiles_data[measure.slug] = chart_data
                        elif trust_series:
                            trust_percentiles_data[measure.slug] = {
                                'trust_count': trust_count,
                                'trustSeries': trust_series,
                            }
                    cache.set(
                        cache_key,
                        (national_data, region_data, trust_percentiles_data),
                        timeout=MEASURES_LIST_CHART_CACHE_TIMEOUT,
                    )
                modes_by_slug = {m.slug: selected_mode for m in measures}
                prefetched = {
                    'national': national_data,
                    'region': region_data,
                    'trust_percentiles': trust_percentiles_data,
                    'modes_by_slug': modes_by_slug,
                }
            context["chart_data_json"] = json.dumps(prefetched, cls=DjangoJSONEncoder)
            if preview_mode:
                context["measures_json"] = "[]"
                context["preview_measures_json"] = _serialize_measures(
                    preview_measures, 'viewer:measure_preview_item', prefetched
                )
                context["in_development_measures_json"] = _serialize_measures(
                    in_development_measures, 'viewer:measure_preview_item', prefetched
                )
            else:
                context["measures_json"] = _serialize_measures(
                    measures, 'viewer:measure_item', prefetched
                )
                context["preview_measures_json"] = "[]"
                context["in_development_measures_json"] = "[]"

        context["measures"] = measures if not preview_mode else []
        context["preview_measures"] = preview_measures
        context["in_development_measures"] = in_development_measures
        context["measure_tags"] = measure_tags
        context["preview_mode"] = preview_mode
        return context


class BaseMeasureItemView(TemplateView):
    template_name = "measure_item.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        slug = self.kwargs.get("slug")

        try:
            measure = self.get_measure(slug)
            context.update(self.get_measure_context(measure))
            precomputed, _ = self.get_precomputed_data(measure)
            context.update(precomputed)
            context['is_new'] = _is_measure_new(measure)
        except Exception as e:
            context["error"] = str(e)

        return context

    def get_measure(self, slug):
        return Measure.objects.prefetch_related('tags').get(slug=slug)

    def get_measure_context(self, measure):
        markdowner = Markdown()

        measure_vmps = MeasureVMP.objects.filter(
            measure=measure
        ).select_related('vmp').values(
            'vmp__code',
            'vmp__name',
            'type',
            'unit'
        )
        
        denominator_vmps = []
        numerator_vmps = []
        for vmp in measure_vmps:
            item = {'name': vmp['vmp__name'], 'code': vmp['vmp__code'], 'unit': vmp['unit']}
            if vmp['type'] == 'denominator':
                denominator_vmps.append(item)
            elif vmp['type'] == 'numerator':
                numerator_vmps.append(item)

        annotations = MeasureAnnotation.objects.filter(
            measure=measure
        ).values('date', 'label', 'description', 'colour')
        
        annotations_data = [
            {
                'date': annotation['date'].isoformat(),
                'label': annotation['label'],
                'description': annotation['description'],
                'colour': annotation['colour'],
            }
            for annotation in annotations
        ]

        tags_data = [
            {
                'name': tag.name,
                'description': (
                    markdowner.convert(tag.description) 
                    if tag.description else None
                ),
                'colour': tag.colour
            }
            for tag in measure.tags.all()
        ]
    
        return {
            "measure_slug": measure.slug,
            "measure_name": measure.name,
            "measure_name_short": measure.short_name,
            "status": measure.status,
            "why_it_matters": markdowner.convert(measure.why_it_matters),
            "how_is_it_calculated": (
                markdowner.convert(measure.how_is_it_calculated)
            ),
            "measure_description": markdowner.convert(measure.description),
            "tags": tags_data,
            "denominator_vmps": json.dumps(
                denominator_vmps, cls=DjangoJSONEncoder
            ),
            "numerator_vmps": json.dumps(
                numerator_vmps, cls=DjangoJSONEncoder
            ),
            "measure_quantity_type": measure.quantity_type,
            "has_denominators": len(denominator_vmps) > 0,
            "annotations": json.dumps(annotations_data, cls=DjangoJSONEncoder),
            "default_view_mode": measure.default_view_mode,
            "lower_is_better": measure.lower_is_better,
        }

    def get_precomputed_data(self, measure):
        cache_key = f'{MEASURE_ITEM_PRECOMPUTED_CACHE_KEY_PREFIX}{measure.slug}'
        cached = cache.get(cache_key)
        if cached is not None:
            return cached, True

        org_measures = PrecomputedMeasure.objects.filter(
            measure=measure
        ).select_related('organisation')
        
        aggregated_measures = PrecomputedMeasureAggregated.objects.filter(
            measure=measure
        )
        
        percentiles = PrecomputedPercentile.objects.filter(
            measure=measure
        )

        context = {}
        context.update(self.get_org_data(org_measures))
        context.update(self.get_aggregated_data(aggregated_measures))
        context.update(self.get_percentile_data(percentiles))

        cache.set(cache_key, context, timeout=MEASURE_ITEM_PRECOMPUTED_CACHE_TIMEOUT)
        return context, False

    def get_org_data(self, org_measures):
        total_orgs = Organisation.objects.count()
        shared_org_data = get_organisation_data()
        org_data = build_measure_org_data(org_measures, shared_org_data, include_region_icb=False)
        org_data_for_json = {k: v for k, v in org_data.items() if k != 'available_count'}
        org_data_for_json.update({
            'trust_types': shared_org_data.get('trust_types', {}),
            'org_regions': shared_org_data.get('org_regions', {}),
            'org_icbs': shared_org_data.get('org_icbs', {}),
            'regions_hierarchy': shared_org_data.get('regions_hierarchy', []),
        })
        return {
            "trusts_included": {"included": org_data['available_count'], "total": total_orgs},
            "org_data": json.dumps(org_data_for_json, cls=DjangoJSONEncoder),
        }

    def get_aggregated_data(self, aggregated_measures):
        region_data = defaultdict(lambda: {'name': '', 'code': '', 'data': []})
        icb_data = defaultdict(lambda: {'name': '', 'code': '', 'data': []})
        national_data = {'name': 'National', 'data': []}
        region_codes = {region.name: region.code for region in Region.objects.all()}
        icb_codes = {icb.name: icb.code for icb in ICB.objects.all()}

        for measure in aggregated_measures:
            if measure.category == 'region':
                region_data[measure.label]['name'] = measure.label
                region_data[measure.label]['code'] = region_codes.get(measure.label, '')
                region_data[measure.label]['data'].append({
                    'month': measure.month,
                    'quantity': measure.quantity,
                    'numerator': measure.numerator,
                    'denominator': measure.denominator
                })
            elif measure.category == 'icb':
                icb_data[measure.label]['name'] = measure.label
                icb_data[measure.label]['code'] = icb_codes.get(measure.label, '')
                icb_data[measure.label]['data'].append({
                    'month': measure.month,
                    'quantity': measure.quantity,
                    'numerator': measure.numerator,
                    'denominator': measure.denominator
                })
            elif measure.category == 'national':
                national_data['data'].append({
                    'month': measure.month,
                    'quantity': measure.quantity,
                    'numerator': measure.numerator,
                    'denominator': measure.denominator
                })

        region_list = list(region_data.values())
        icb_list = list(icb_data.values())

        return {
            "region_data": json.dumps(region_list, cls=DjangoJSONEncoder),
            "icb_data": json.dumps(icb_list, cls=DjangoJSONEncoder),
            "national_data": json.dumps(national_data, cls=DjangoJSONEncoder),
        }

    def get_percentile_data(self, percentiles):
        percentiles_list = list(percentiles.values())
        return {
            "percentile_data": json.dumps(
                percentiles_list, cls=DjangoJSONEncoder
            ),
        }


class MeasureTrustsView(LoginRequiredMixin, MaintenanceModeMixin, TemplateView):
    """View showing one percentile chart per trust for a given measure."""

    template_name = "measure_trusts.html"
    login_url = reverse_lazy("viewer:login")

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        slug = self.kwargs.get("slug")

        try:
            measure = Measure.objects.prefetch_related('tags').get(slug=slug)
            org_measures = PrecomputedMeasure.objects.filter(
                measure=measure
            ).select_related('organisation')
            percentiles = PrecomputedPercentile.objects.filter(measure=measure)
            shared_org_data = get_organisation_data()

            org_data = build_measure_org_data(org_measures, shared_org_data, include_region_icb=True)

            org_data_for_json = {
                k: v for k, v in org_data.items()
                if k not in ('available_count',)
            }
            org_data_for_json.update({
                'trust_types': shared_org_data.get('trust_types', {}),
                'org_regions': shared_org_data.get('org_regions', {}),
                'org_icbs': shared_org_data.get('org_icbs', {}),
            })
            percentile_data = list(
                percentiles.values('month', 'percentile', 'quantity')
            )
            for p in percentile_data:
                p['month'] = p['month'].isoformat()

            denom_exists = MeasureVMP.objects.filter(
                measure=measure, type='denominator'
            ).exists()

            markdowner = Markdown()
            tags_data = [
                {
                    "name": tag.name,
                    "description": (
                        markdowner.convert(tag.description)
                        if tag.description
                        else None
                    ),
                    "colour": tag.colour or "#6b7280",
                }
                for tag in measure.tags.all()
            ]
            context.update({
                "measure": measure,
                "measure_description": markdowner.convert(measure.description),
                "tags": tags_data,
                "trusts": org_data['organisations'],
                "org_data_json": json.dumps(
                    org_data_for_json,
                    cls=DjangoJSONEncoder,
                ),
                "percentile_data_json": json.dumps(
                    percentile_data, cls=DjangoJSONEncoder
                ),
                "regions_hierarchy_json": json.dumps(
                    org_data.get('regions_hierarchy', []),
                    cls=DjangoJSONEncoder,
                ),
                "measure_has_denominators": denom_exists,
                "measure_quantity_type": measure.quantity_type or "",
                "measure_lower_is_better": (
                    "true"
                    if measure.lower_is_better is True
                    else "false"
                    if measure.lower_is_better is False
                    else ""
                ),
                "selected_sort": self.request.GET.get("sort", "name"),
            })
        except Measure.DoesNotExist:
            context["error"] = "Measure not found"
        except Exception as e:
            context["error"] = str(e)

        return context


class MeasureItemView(MaintenanceModeMixin, BaseMeasureItemView):
    """View for published measures only"""

    def dispatch(self, request, *args, **kwargs):
        try:
            slug = kwargs.get("slug")
            measure = self.get_measure(slug)
            
            if measure.status != 'published':
                return redirect('viewer:measures_list')
            
            return super().dispatch(request, *args, **kwargs)
            
        except Exception:
            return redirect('viewer:measures_list')


class MeasurePreviewItemView(MaintenanceModeMixin, BaseMeasureItemView):
    """View for preview and in-development measures"""

    def dispatch(self, request, *args, **kwargs):
        try:
            slug = kwargs.get("slug")
            measure = self.get_measure(slug)
            
            if (measure.status == 'in_development' and 
                    not request.user.is_authenticated):
                return redirect('viewer:measures_list')
            elif measure.status == 'published':
                return redirect('viewer:measure_item', slug=slug)
            
            return super().dispatch(request, *args, **kwargs)
            
        except Exception:
            return redirect('viewer:measures_list')