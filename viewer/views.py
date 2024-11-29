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
from django.db.models import Prefetch
from django.db.models.functions import Coalesce
from django.db.models import F, Value, Min, Max
from django.utils.safestring import mark_safe
from django.db.models import Exists, OuterRef
from django.db.models import Count, Q, Prefetch
from django.views.decorators.csrf import csrf_protect
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.contrib.auth.views import LoginView as AuthLoginView


from .models import (
    ATC,
    Dose,
    Organisation,
    VMP,
    IngredientQuantity,
    Ingredient,
    VTM,
    Measure,
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
        date_range = Dose.objects.aggregate(
            min_date=Min('year_month'),
            max_date=Max('year_month')
        )
        context['date_range'] = {
            'min_date': date_range['min_date'].isoformat() if date_range['min_date'] else None,
            'max_date': date_range['max_date'].isoformat() if date_range['max_date'] else None
        }

        ods_data = Organisation.objects.values('ods_name', 'ods_code').distinct().order_by('ods_name')
        ods_data = [f"{org['ods_code']} | {org['ods_name']}" for org in ods_data]
        context['ods_data'] = json.dumps(ods_data, default=str)
        return context

@method_decorator(login_required, name='dispatch')
class MeasuresListView(TemplateView):
    template_name = "measures_list.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        measures = Measure.objects.select_related('reason').filter(draft=False)
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
        
        if measure.quantity_type == 'dose':
            Model = Dose
        else:
            Model = IngredientQuantity
        
        denominator_vmps = []
        for vmp in measure.denominator_vmps.all():
            unit = Model.objects.filter(vmp=vmp).values('unit').first()
            denominator_vmps.append({
                'name': vmp.name,
                'code': vmp.code,
                'unit': unit['unit'] if unit else None
            })
        
        numerator_vmps = []
        for vmp in measure.numerator_vmps.all():
            unit = Model.objects.filter(vmp=vmp).values('unit').first()
            numerator_vmps.append({
                'name': vmp.name,
                'code': vmp.code,
                'unit': unit['unit'] if unit else None
            })
        
        return {
            "measure_name": measure.name,
            "measure_name_short": measure.short_name,
            "why_it_matters": markdowner.convert(measure.why_it_matters),
            "how_is_it_calculated": markdowner.convert(measure.how_is_it_calculated),
            "measure_description": markdowner.convert(measure.description),
            "reason": measure.reason.reason if measure.reason else None,
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
        all_orgs = set(org_measures.values_list('organisation__ods_name', flat=True).distinct())
        
        non_zero_orgs = set(org_measures.values('organisation__ods_code')
                        .annotate(non_zero_count=Count('id', 
                            filter=Q(denominator__isnull=False) & ~Q(denominator=0)))
                        .filter(non_zero_count__gt=0)
                        .values_list('organisation__ods_code', flat=True))
        
        org_measures_dict = {}
        for measure in org_measures.filter(organisation__ods_code__in=non_zero_orgs).values(
            'organisation__ods_code', 'organisation__ods_name', 'month', 'quantity', 'numerator', 'denominator'
        ):
            org_key = f"{measure['organisation__ods_code']} | {measure['organisation__ods_name']}"
            org_measures_dict.setdefault(org_key, []).append({
                'month': measure['month'],
                'quantity': measure['quantity'],
                'numerator': measure['numerator'],
                'denominator': measure['denominator']
            })

        return {
            "trusts_included": {
                "included": len(non_zero_orgs),
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


def get_all_child_atc_codes(atc_codes):
    all_codes = set(atc_codes)
    
    for code in atc_codes:
        children = ATC.objects.filter(code__startswith=code).exclude(code=code).values_list('code', flat=True)
        all_codes.update(children)
    
    return list(all_codes)

@login_required
@csrf_protect
@api_view(["POST"])
def filtered_quantities(request):
    search_items = request.data.get("names", [])
    ods_names = request.data.get("ods_names", [])
    search_type = request.data.get("search_type", "vmp")
    start_date = request.data.get("start_date")
    end_date = request.data.get("end_date")
    quantity_type = request.data.get("quantity_type", "VMP Quantity")
        
    search_items = [item.split("|")[0].strip() for item in search_items]

    base_filters = {}
    
    if start_date:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        base_filters['year_month__gte'] = start_date
        
    if end_date:
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        base_filters['year_month__lte'] = end_date

    if quantity_type == "Ingredient Quantity":
        Model = IngredientQuantity
    elif quantity_type == "VMP Quantity":
        Model = SCMDQuantity
    else:
        Model = Dose

    queryset = Model.objects.filter(**base_filters)
    if search_type == "product":
        queryset = queryset.filter(Q(vmp__code__in=search_items) | Q(vmp__vtm__vtm__in=search_items))
    elif search_type == "ingredient":
        if quantity_type == "Ingredient Quantity":
            queryset = queryset.filter(ingredient__code__in=search_items)
        else:
            queryset = queryset.filter(vmp__ingredients__code__in=search_items)
    elif search_type == "atc":
        all_atc_codes = get_all_child_atc_codes(search_items)
        queryset = queryset.filter(vmp__atcs__code__in=all_atc_codes)

    if ods_names:
        ods_names = [item.split("|")[0].strip() for item in ods_names]
        queryset = queryset.filter(organisation__ods_code__in=ods_names)

    vmp_atc_map = {
        iq.id: [{'code': atc.code, 'name': atc.name} for atc in iq.vmp.atcs.all()]
        for iq in queryset.select_related('vmp').prefetch_related('vmp__atcs')
    }

    value_fields = [
        "id",
        "year_month",
        "quantity",
        "unit",
        "vmp__code",
        "vmp__name",
        "organisation__ods_code",
        "organisation__ods_name",
        "vmp__vtm__name",
    ]

    if search_type == "ingredient" or quantity_type == "Ingredient Quantity":
        if quantity_type == "Ingredient Quantity":
            value_fields.extend([
                "ingredient__code",
                "ingredient__name"
            ])
        else:
            value_fields.extend([
                "vmp__ingredients__code",
                "vmp__ingredients__name"
            ])

    raw_data = list(
        queryset.values(*value_fields)
        .order_by("year_month", "vmp__name", "organisation__ods_name")
    )
    
    vmp_route_map = {
        iq.id: [{'code': route.code, 'name': route.name} for route in iq.vmp.routes.all()]
        for iq in queryset.select_related('vmp').prefetch_related('vmp__routes')
    }

    data = []
    for item in raw_data:
        try:
            atc_info = vmp_atc_map[item["id"]]
            route_info = vmp_route_map[item["id"]]
            processed_item = {
                "id": item["id"],
                "year_month": item["year_month"].strftime("%Y-%m-%d"),
                "quantity": round(item["quantity"], 6) if item["quantity"] is not None and not math.isnan(item["quantity"]) else None,
                "unit": item["unit"],
                "vmp_code": item["vmp__code"],
                "vmp_name": item["vmp__name"],
                "ods_code": item["organisation__ods_code"],
                "ods_name": item["organisation__ods_name"],
                "vtm_name": item["vmp__vtm__name"] or "",
                "atc_code": atc_info[0]['code'] if atc_info else "",
                "atc_name": atc_info[0]['name'] if atc_info else "Unknown ATC",
                "route_codes": [r['code'] for r in route_info] if route_info else [],
                "route_names": [r['name'] for r in route_info] if route_info else ["Unknown Route"],
            }

            if search_type == "ingredient" or quantity_type == "Ingredient Quantity":
                if quantity_type == "Ingredient Quantity":
                    processed_item.update({
                        "ingredient_code": item.get("ingredient__code"),
                        "ingredient_name": item.get("ingredient__name")
                    })
                else:
                    processed_item.update({
                        "ingredient_code": item.get("vmp__ingredients__code"),
                        "ingredient_name": item.get("vmp__ingredients__name")
                    })

            data.append(processed_item)
        except Exception as e:
            print(f"Error processing item: {e}")
            continue
    
    return Response(data)


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
    elif search_type == "atc":
        all_atc_codes = get_all_child_atc_codes(search_items)
        queryset = VMP.objects.filter(atcs__code__in=all_atc_codes)
        
        # If no VMPs found for any ATC code, return 0
        if not queryset.exists():
            return Response({"vmp_count": 0})
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
    
    elif search_type == 'atc':
        vmp_exists = VMP.objects.filter(atcs__code=OuterRef('code')).values('code')
        items = ATC.objects.filter(
            Q(name__icontains=search_term) | 
            Q(code__icontains=search_term)
        ).annotate(
            has_vmps=Exists(vmp_exists)
        ).values('code', 'name', 'has_vmps').distinct().order_by('code')[:50]
        
        return JsonResponse({
            'results': [{
                'code': item['code'],
                'name': f"{item['code']} | {item['name']}",
                'has_vmps': item['has_vmps']
            } for item in items]
        })
    
    return JsonResponse({'results': []})

@method_decorator(login_required, name='dispatch')
class FAQView(TemplateView):
    template_name = "faq.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context

