import json
import math
from collections import defaultdict
from datetime import date, datetime
from markdown2 import Markdown
from django.views.generic import TemplateView
from rest_framework.decorators import api_view
from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.response import Response
from django.http import JsonResponse
from django.db.models.functions import Coalesce
from django.db.models import F, Value, Min, Max
from django.utils.safestring import mark_safe
from django.db.models import Exists, OuterRef, Q, Subquery, OuterRef
from django.views.decorators.csrf import csrf_protect
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.contrib.auth.views import LoginView as AuthLoginView
from django.shortcuts import redirect
from django.contrib.postgres.aggregates import ArrayAgg

from .models import (
    Organisation,
    VMP,
    IngredientQuantity,
    Ingredient,
    VTM,
    Measure,
    MeasureVMP,
    PrecomputedMeasure,
    PrecomputedMeasureAggregated,
    PrecomputedPercentile,
    OrgSubmissionCache,
    DataStatus,
    MeasureReason,
    SCMDQuantity,
    
)


@method_decorator(login_required, name='dispatch')
class IndexView(TemplateView):
    template_name = "index.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context

class LoginView(AuthLoginView):
    template_name = 'login.html'

@method_decorator(login_required, name='dispatch')
class AnalyseView(TemplateView):
    template_name = "analyse.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
      
        ods_data = Organisation.objects.values('ods_name', 'ods_code').distinct().order_by('ods_name')
        ods_data = [f"{org['ods_code']} | {org['ods_name']}" for org in ods_data]
        context['ods_data'] = json.dumps(ods_data, default=str)
        return context

@method_decorator(login_required, name='dispatch')
class MeasuresListView(TemplateView):
    template_name = "measures_list.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        measures = Measure.objects.select_related('reason').order_by('draft', 'name')
        measure_reasons = MeasureReason.objects.all()
        
        markdowner = Markdown()
        for measure in measures:
            measure.why_it_matters = markdowner.convert(measure.why_it_matters)
        
        for reason in measure_reasons:
            if reason.description:
                reason.description = markdowner.convert(reason.description)
        
        context["measures"] = measures
        context["measure_reasons"] = measure_reasons
        return context


@method_decorator(login_required, name='dispatch')
class MeasureItemView(TemplateView):
    template_name = "measure_item.html"

    def dispatch(self, request, *args, **kwargs):
        try:
            slug = kwargs.get("slug")
            measure = self.get_measure(slug)
            if measure.draft:
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
        except Exception as e:
            context["error"] = str(e)

        return context

    def get_measure(self, slug):
        return Measure.objects.select_related('reason').get(slug=slug)

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
        
        return {
            "measure_name": measure.name,
            "measure_name_short": measure.short_name,
            "why_it_matters": markdowner.convert(measure.why_it_matters),
            "how_is_it_calculated": markdowner.convert(measure.how_is_it_calculated),
            "measure_description": markdowner.convert(measure.description),
            "reason": measure.reason.reason if measure.reason else None,
            "reason_description": markdowner.convert(measure.reason.description) if measure.reason else None,
            "reason_colour": measure.reason.colour if measure.reason else None,
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
        # Get all active orgs (those with no successor)
        all_orgs = Organisation.objects.filter(
            successor__isnull=True
        ).values('ods_code', 'ods_name').order_by('ods_name')
        
        # Create a set of orgs that are included in the measure
        measure_orgs = set(org_measures.values_list('organisation__ods_code', flat=True).distinct())
        
        org_measures_dict = {}
       
        for org in all_orgs:
            org_key = f"{org['ods_code']} | {org['ods_name']}"
            org_measures_dict[org_key] = {
                'available': org['ods_code'] in measure_orgs,
                'data': []
            }
        
        for measure in org_measures.values(
            'organisation__ods_code', 'organisation__ods_name', 'month', 'quantity', 'numerator', 'denominator'
        ):
            org_key = f"{measure['organisation__ods_code']} | {measure['organisation__ods_name']}"
            org_measures_dict[org_key]['data'].append({
                'month': measure['month'],
                'quantity': measure['quantity'],
                'numerator': measure['numerator'],
                'denominator': measure['denominator']
            })

        return {
            "trusts_included": {
                "included": len(measure_orgs),
                "total": len(all_orgs)
            },
            "org_data": json.dumps(org_measures_dict, cls=DjangoJSONEncoder),
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

@login_required
@csrf_protect
@api_view(["POST"])
def filtered_quantities(request):
    search_items = request.data.get("names", None)
    ods_names = request.data.get("ods_names", None)
    search_type = request.data.get("search_type", None)
    quantity_type = request.data.get("quantity_type", None)
    

    if search_items is None:
        return Response({"error": "No search items provided"}, status=400)
    
    if search_type == "product":
        vmp_query = VMP.objects.filter(
            Q(code__in=search_items) | Q(vtm__vtm__in=search_items)
        )
        vmp_ids = vmp_query.values_list('id', flat=True)
    
    elif search_type == "ingredient":
        vmp_query = VMP.objects.filter(ingredients__code__in=search_items)
        vmp_ids = vmp_query.values_list('id', flat=True)
    
    else:
        return Response({"error": "Invalid search type"}, status=400)
    
    if quantity_type == "VMP Quantity":
        if ods_names:
            ods_names = [item.split("|")[0].strip() for item in ods_names]
            queryset = SCMDQuantity.objects.filter(vmp_id__in=vmp_ids, organisation__ods_code__in=ods_names)
        else:
            queryset = SCMDQuantity.objects.filter(vmp_id__in=vmp_ids)
        
    elif quantity_type == "Ingredient Quantity":
        if ods_names:
            ods_names = [item.split("|")[0].strip() for item in ods_names]
            queryset = IngredientQuantity.objects.filter(vmp_id__in=vmp_ids, organisation__ods_code__in=ods_names).select_related('ingredient')
        else:
            queryset = IngredientQuantity.objects.filter(vmp_id__in=vmp_ids).select_related('ingredient')
    
    else:
        return Response({"error": "Invalid quantity type"}, status=400)
    

    value_fields = [
        'data',
        'vmp__code',
        'vmp__name',
        'vmp__vtm__name',
        'organisation__ods_code',
        'organisation__ods_name',
    ]
    
    if search_type == "ingredient" or quantity_type == "Ingredient Quantity":
        data = queryset.annotate(
            route_names=ArrayAgg('vmp__routes__name', distinct=True),
            ingredient_names=ArrayAgg('vmp__ingredients__name', distinct=True)
        ).values(*value_fields, 'route_names', 'ingredient_names')
    else:
        data = queryset.annotate(
            route_names=ArrayAgg('vmp__routes__name', distinct=True)
        ).values(*value_fields, 'route_names')

    data_list = list(data)
    included_vmps = {item['vmp__code'] for item in data_list}
    
    # Get missing VMPs
    missing_vmps = VMP.objects.filter(
        id__in=vmp_ids
    ).exclude(
        code__in=included_vmps
    ).annotate(
        route_names=ArrayAgg('routes__name', distinct=True)
    )
    
    if search_type == "ingredient" or quantity_type == "Ingredient Quantity":
        missing_vmps = missing_vmps.annotate(
            ingredient_names=ArrayAgg('ingredients__name', distinct=True)
        )
    
    # Add missing VMPs to the response with empty data - this is shown in the Product List table
    for vmp in missing_vmps.values('code', 'name', 'vtm__name', 'route_names', *(['ingredient_names'] if search_type == "ingredient" or quantity_type == "Ingredient Quantity" else [])):
        empty_vmp = {
            'data': [],
            'vmp__code': vmp['code'],
            'vmp__name': vmp['name'],
            'vmp__vtm__name': vmp['vtm__name'],
            'organisation__ods_code': None,
            'organisation__ods_name': None,
            'route_names': vmp['route_names']
        }
        if search_type == "ingredient" or quantity_type == "Ingredient Quantity":
            empty_vmp['ingredient_names'] = vmp['ingredient_names']
        data_list.append(empty_vmp)
    
    return Response(data_list)

@method_decorator(login_required, name='dispatch')
class OrgsSubmittingDataView(TemplateView):
    template_name = 'org_submissions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Get latest dates for each file type
        latest_dates = {}
        for file_type in ['final']:
            latest = DataStatus.objects.filter(
                file_type=file_type
            ).aggregate(
                latest_date=Max('year_month')
            )['latest_date']
            if latest:
                latest_dates[file_type] = latest.strftime("%B %Y")
            else:
                latest_dates[file_type] = None
        
        context['latest_dates'] = json.dumps(latest_dates)

        # Step 1: Collect data
        org_data = defaultdict(lambda: {'successor': None, 'submissions': {}, 'predecessors': []})
        all_dates = set()
        
        for cache in OrgSubmissionCache.objects.select_related('organisation', 'successor').order_by('organisation__ods_name', 'month'):
            org_name = cache.organisation.ods_name
            org_data[org_name]['successor'] = cache.successor.ods_name if cache.successor else None
            month_str = cache.month.isoformat() if isinstance(cache.month, date) else str(cache.month)
            org_data[org_name]['submissions'][month_str] = {
                'has_submitted': cache.has_submitted,
                'vmp_count': cache.vmp_count or 0
            }
            all_dates.add(month_str)

        # Step 2: Build predecessor relationships
        for org_name, data in org_data.items():
            if data['successor']:
                org_data[data['successor']]['predecessors'].append(org_name)

        # Step 3: Restructure data
        restructured_data = []
        processed_orgs = set()

        def build_org_hierarchy(org_name):
            org_entry = {
                'name': org_name,
                'data': org_data[org_name]['submissions'],
                'predecessors': []
            }
            processed_orgs.add(org_name)

            # Sort predecessors to ensure consistent ordering
            sorted_predecessors = sorted(org_data[org_name]['predecessors'])
            for pred in sorted_predecessors:
                if pred not in processed_orgs:
                    org_entry['predecessors'].append(build_org_hierarchy(pred))

            return org_entry

        # Process current organizations (those without successors) first
        current_orgs = sorted([org for org, data in org_data.items() if not data['successor']])
        for org in current_orgs:
            if org not in processed_orgs:
                restructured_data.append(build_org_hierarchy(org))

        # Process any remaining organizations
        for org in sorted(org_data.keys()):
            if org not in processed_orgs:
                restructured_data.append(build_org_hierarchy(org))

        latest_date = max(all_dates) if all_dates else None
        for org_entry in restructured_data:
            org_entry['latest_submission'] = org_entry['data'].get(latest_date, False)

        context['org_data_json'] = mark_safe(json.dumps(restructured_data))

        if all_dates:
            earliest_date = min(all_dates)
            latest_date = max(all_dates)
            
            context['earliest_date'] = datetime.strptime(earliest_date, "%Y-%m-%d").strftime("%B %Y")
            context['latest_date'] = datetime.strptime(latest_date, "%Y-%m-%d").strftime("%B %Y")
        else:
            context['earliest_date'] = None
            context['latest_date'] = None

        return context


@login_required
@api_view(["POST"])
def filtered_vmp_count(request):
    search_items = request.data.get("names", [])
    search_type = request.data.get("search_type", "vmp")

    search_items = [item.split("|")[0].strip() for item in search_items]
    
    if search_type == "product":
        # Handle both VMP and VTM codes
        vtm_codes = []
        vmp_codes = []
        for code in search_items:
            if VTM.objects.filter(vtm=code).exists():
                vtm_codes.append(code)
            else:
                vmp_codes.append(code)
        
        # Get VMPs directly selected and VMPs under selected VTMs
        queryset = VMP.objects.filter(
            Q(code__in=vmp_codes) |
            Q(vtm__vtm__in=vtm_codes)
        )

        # Get display names for both VTMs and VMPs
        vtms = VTM.objects.filter(vtm__in=vtm_codes).values('vtm', 'name')
        vmps = VMP.objects.filter(code__in=vmp_codes).values('code', 'name')
        
        display_names = {}
        # Add VTM display names
        for vtm in vtms:
            display_names[vtm['vtm']] = f"{vtm['name']} ({vtm['vtm']})"
        # Add VMP display names
        for vmp in vmps:
            display_names[vmp['code']] = f"{vmp['name']} ({vmp['code']})"
        
        return Response({
            "vmp_count": queryset.distinct().count(),
            "display_names": display_names
        })
    
    elif search_type == "ingredient":
        queryset = VMP.objects.filter(ingredients__code__in=search_items)

    else:
        return Response({"vmp_count": 0})
    
    vmp_count = queryset.distinct().count()
    return Response({"vmp_count": vmp_count})


@method_decorator(login_required, name='dispatch')
class ContactView(TemplateView):
    template_name = "contact.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context
    
@login_required
@api_view(["GET"])
def search_items(request):
    search_type = request.GET.get('type', 'product')
    search_term = request.GET.get('term', '').lower()
    
    if search_type == 'product':
        # First get matching VMPs (both with and without VTMs)
        matching_vmps = VMP.objects.filter(
            Q(name__icontains=search_term) | 
            Q(code__icontains=search_term)
        ).select_related('vtm')

        # Organize VMPs by VTM
        vmp_by_vtm = {}
        standalone_vmps = []
        
        for vmp in matching_vmps:
            if vmp.vtm:
                vtm_key = vmp.vtm.vtm  # Using VTM code as key
                if vtm_key not in vmp_by_vtm:
                    vmp_by_vtm[vtm_key] = {
                        'vtm': vmp.vtm,
                        'vmps': []
                    }
                vmp_by_vtm[vtm_key]['vmps'].append(vmp)
            else:
                standalone_vmps.append(vmp)

        # Get additional VTMs that match the search term
        additional_vtms = VTM.objects.filter(
            Q(name__icontains=search_term) | 
            Q(vtm__icontains=search_term)
        ).exclude(vtm__in=vmp_by_vtm.keys())

        # Build results
        results = []
        
        # Add VTMs with their VMPs
        for vtm_data in vmp_by_vtm.values():
            vtm = vtm_data['vtm']
            results.append({
                'code': vtm.vtm,
                'name': vtm.name,
                'type': 'vtm',
                'isExpanded': False,
                'display_name': f"{vtm.name} ({vtm.vtm})",
                'vmps': [{
                    'code': vmp.code,
                    'name': vmp.name,
                    'type': 'vmp',
                    'display_name': f"{vmp.name} ({vmp.code})"
                } for vmp in vtm_data['vmps']]
            })

        # Add additional VTMs with their VMPs
        for vtm in additional_vtms:
            vmps = vtm.vmps.all()
            results.append({
                'code': vtm.vtm,
                'name': vtm.name,
                'type': 'vtm',
                'isExpanded': False,
                'display_name': f"{vtm.name} ({vtm.vtm})",
                'vmps': [{
                    'code': vmp.code,
                    'name': vmp.name,
                    'type': 'vmp',
                    'display_name': f"{vmp.name} ({vmp.code})"
                } for vmp in vmps]
            })

        # Add standalone VMPs
        results.extend([{
            'code': vmp.code,
            'name': vmp.name,
            'type': 'vmp',
            'display_name': f"{vmp.name} ({vmp.code})"
        } for vmp in standalone_vmps])

        return JsonResponse({'results': results})
    
    elif search_type == 'ingredient':
        items = Ingredient.objects.filter(
            Q(name__icontains=search_term) | 
            Q(code__icontains=search_term)
        ).values('name', 'code').distinct().order_by('name')[:50]
        return JsonResponse({
            'results': [{
                'code': item['code'],
                'name': item['name'],
                'type': 'ingredient'
            } for item in items]
        })
    

    
    return JsonResponse({'results': []})

@method_decorator(login_required, name='dispatch')
class FAQView(TemplateView):
    template_name = "faq.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context

