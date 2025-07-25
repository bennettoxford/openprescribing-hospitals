import json
from collections import defaultdict
from datetime import datetime, timedelta
from markdown2 import Markdown
from django.views.generic import TemplateView
from django.core.serializers.json import DjangoJSONEncoder
from django.shortcuts import redirect

from ..models import (
    Measure,
    MeasureVMP,
    MeasureTag,
    PrecomputedMeasure,
    PrecomputedMeasureAggregated,
    PrecomputedPercentile,
    Organisation,
)

class MeasuresListView(TemplateView):
    template_name = "measures_list.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        measures = Measure.objects.prefetch_related('tags').order_by('draft', 'name')
        measure_tags = MeasureTag.objects.all()
        
        markdowner = Markdown()
        for measure in measures:
            measure.why_it_matters = markdowner.convert(measure.why_it_matters)
        
        for tag in measure_tags:
            if tag.description:
                tag.description = markdowner.convert(tag.description)
        
        one_month_ago = datetime.now().date() - timedelta(days=30)
        for measure in measures:
            measure.is_new = measure.first_published and measure.first_published >= one_month_ago
        
        context["measures"] = measures
        context["measure_tags"] = measure_tags
        return context
    
class MeasureItemView(TemplateView):
    template_name = "measure_item.html"

    def dispatch(self, request, *args, **kwargs):
        try:
            slug = kwargs.get("slug")
            measure = self.get_measure(slug)
            if measure.draft:
                if request.user.is_authenticated:
                    return super().dispatch(request, *args, **kwargs)
                else:
                    return redirect('viewer:measures_list')
        except Exception:
            pass
        
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        slug = self.kwargs.get("slug")

        try:
            measure = self.get_measure(slug)
            context.update(self.get_measure_context(measure))
            context.update(self.get_precomputed_data(measure))
            
            is_new = False
            if measure.first_published:
                one_month_ago = datetime.now().date() - timedelta(days=30)
                is_new = measure.first_published >= one_month_ago
            
            context['is_new'] = is_new
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
        
        denominator_vmps = [
            {
                'name': vmp['vmp__name'],
                'code': vmp['vmp__code'],
                'unit': vmp['unit']
            }
            for vmp in measure_vmps
            if vmp['type'] == 'denominator'
        ]
        
        numerator_vmps = [
            {
                'name': vmp['vmp__name'],
                'code': vmp['vmp__code'],
                'unit': vmp['unit']
            }
            for vmp in measure_vmps
            if vmp['type'] == 'numerator'
        ]

        tags_data = [
            {
                'name': tag.name,
                'description': markdowner.convert(tag.description) if tag.description else None,
                'colour': tag.colour
            }
            for tag in measure.tags.all()
        ]
    
        return {
            "measure_name": measure.name,
            "measure_name_short": measure.short_name,
            "is_draft": measure.draft,
            "why_it_matters": markdowner.convert(measure.why_it_matters),
            "how_is_it_calculated": markdowner.convert(measure.how_is_it_calculated),
            "measure_description": markdowner.convert(measure.description),
            "tags": tags_data,
            "denominator_vmps": json.dumps(denominator_vmps, cls=DjangoJSONEncoder),
            "numerator_vmps": json.dumps(numerator_vmps, cls=DjangoJSONEncoder),
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

        current_orgs = Organisation.objects.filter(
            successor__isnull=True
        ).values('ods_code', 'ods_name').order_by('ods_name')
        
        measure_orgs = set(org_measures.values_list('organisation__ods_code', flat=True).distinct())
        
        predecessor_map = {}
        predecessor_to_successor = {}
 
        for org in Organisation.objects.exclude(successor__isnull=True).values(
            'ods_code', 'ods_name', 'successor__ods_code', 'successor__ods_name'
        ):
            successor_name = org['successor__ods_name']
            predecessor_name = org['ods_name']
            
            
            if successor_name not in predecessor_map:
                predecessor_map[successor_name] = []
            predecessor_map[successor_name].append(predecessor_name)
            predecessor_to_successor[predecessor_name] = successor_name
       
        org_data = {
            'data': {},
            'predecessor_map': predecessor_map
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
        
        for successor, predecessors in predecessor_map.items():
            if successor in org_data['data'] and org_data['data'][successor]['available']:
                for predecessor in predecessors:
                    org_data['data'][predecessor] = {
                        'available': True,
                        'data': []
                    }
                    available_orgs.add(predecessor)
        
        for measure in org_measures.values(
            'organisation__ods_code', 'organisation__ods_name', 'month', 'quantity', 'numerator', 'denominator'
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
        region_data = defaultdict(lambda: {'name': '', 'data': []})
        icb_data = defaultdict(lambda: {'name': '', 'data': []})
        national_data = {'name': 'National', 'data': []}

        for measure in aggregated_measures:
            if measure.category == 'region':
                region_data[measure.label]['name'] = measure.label
                region_data[measure.label]['data'].append({
                    'month': measure.month,
                    'quantity': measure.quantity,
                    'numerator': measure.numerator,
                    'denominator': measure.denominator
                })
            elif measure.category == 'icb':
                icb_data[measure.label]['name'] = measure.label
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
            "percentile_data": json.dumps(percentiles_list, cls=DjangoJSONEncoder),
        }