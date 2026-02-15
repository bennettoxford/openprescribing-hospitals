import json
from collections import defaultdict
from datetime import datetime, timedelta
from markdown2 import Markdown
from django.views.generic import TemplateView
from django.core.serializers.json import DjangoJSONEncoder
from django.shortcuts import redirect
from django.urls import reverse
from django.utils.text import slugify
from django.db.models import Exists, OuterRef

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


def get_bulk_trust_series_for_measures(measures, trust_codes):
    """
    Fetch trust series data for a set of measures and trust codes.
    Returns grouped data by measure_id, then by month.
    """
    if not trust_codes:
        return {}

    measure_ids = [m.id for m in measures]
    trust_series = PrecomputedMeasure.objects.filter(
        measure_id__in=measure_ids,
        organisation__ods_code__in=trust_codes
    ).select_related('measure', 'organisation').values(
        'measure_id', 'month', 'quantity', 'numerator', 'denominator'
    ).order_by('measure_id', 'month')

    grouped_data = defaultdict(lambda: defaultdict(list))
    for row in trust_series:
        grouped_data[row['measure_id']][row['month']].append(row)

    aggregated_data = defaultdict(dict)
    for measure_id, months_data in grouped_data.items():
        for month, rows in months_data.items():
            measure = next((m for m in measures if m.id == measure_id), None)
            if not measure:
                continue

            if measure.has_denominators:
                # For percentage measures, sum numerators and denominators, then compute percentage
                total_numerator = sum(row['numerator'] or 0 for row in rows)
                total_denominator = sum(row['denominator'] or 0 for row in rows)
                if total_denominator > 0:
                    value = (total_numerator / total_denominator) * 100
                else:
                    value = 0
                # Clamp percentage values to 0-100%
                value = max(0, min(100, value))
            else:
                # For other measures, sum quantity
                value = sum(row['quantity'] or 0 for row in rows)
                # Clamp to minimum of 0, no upper limit
                value = max(0, value)

            aggregated_data[measure_id][month] = value

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
            selected_mode = (self.request.GET.get('mode') or 'default').strip()
            if selected_mode not in ('national', 'region', 'trust', 'default'):
                selected_mode = 'default'
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

        measure_tags = list(MeasureTag.objects.all().order_by('name'))
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
                bulk_all_regions, bulk_national = get_bulk_regions_and_national_series_for_measures(measures)
                bulk_percentiles = get_bulk_percentiles_for_measures(measures)
                national_data = {}
                region_data = {}
                trust_percentiles_data = {}
                modes_by_slug = {}
                for measure in measures:
                    national_data[measure.slug] = build_national_chart_data(measure, bulk_national)
                    region_data[measure.slug] = build_region_chart_data(measure, bulk_all_regions)
                    trust_percentiles_data[measure.slug] = build_trust_chart_data(measure, bulk_percentiles)
                    if selected_mode == 'default':
                        mode_val = measure.default_view_mode
                        effective_mode = (
                            mode_val if mode_val in ('national', 'region', 'trust')
                            else (None if mode_val == 'icb' else 'national')
                        )
                        modes_by_slug[measure.slug] = effective_mode if effective_mode is not None else 'national'
                    else:
                        modes_by_slug[measure.slug] = selected_mode
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
            context.update(self.get_precomputed_data(measure))
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

        return context

    def get_org_data(self, org_measures):
        
        total_orgs = Organisation.objects.count()
        
        shared_org_data = get_organisation_data()

        current_orgs = Organisation.objects.filter(
            successor__isnull=True
        ).values('ods_code', 'ods_name').order_by('ods_name')
        
        measure_orgs = set(
            org_measures.values_list(
                'organisation__ods_code', flat=True
            ).distinct()
        )
        
        predecessor_to_successor = {}
        for successor, predecessors in shared_org_data['predecessor_map'].items():
            for predecessor in predecessors:
                predecessor_to_successor[predecessor] = successor
       
        org_data = {
            'data': {},
            'predecessor_map': shared_org_data['predecessor_map'],
            'org_codes': shared_org_data['org_codes']
        }
        
        available_orgs = set()
        
        for org in current_orgs:
            org_name = org['ods_name']
            is_available = org['ods_code'] in measure_orgs
            org_data['data'][org_name] = {
                'available': is_available,
                'data': []
            }
            if is_available:
                available_orgs.add(org_name)
        
        for successor, predecessors in shared_org_data['predecessor_map'].items():
            if (successor in org_data['data'] and 
                    org_data['data'][successor]['available']):
                for predecessor in predecessors:
                    org_data['data'][predecessor] = {
                        'available': True,
                        'data': []
                    }
                    available_orgs.add(predecessor)
        
        for measure in org_measures.values(
            'organisation__ods_code', 'organisation__ods_name', 
            'month', 'quantity', 'numerator', 'denominator'
        ):
            org_name = measure['organisation__ods_name']
            if org_name in org_data['data']:
                org_data['data'][org_name]['data'].append({
                    'month': measure['month'],
                    'quantity': measure['quantity'],
                    'numerator': measure['numerator'],
                    'denominator': measure['denominator']
                })
        
        return {
            "trusts_included": {
                "included": len(available_orgs),
                "total": total_orgs
            },
            "org_data": json.dumps(org_data, cls=DjangoJSONEncoder),
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