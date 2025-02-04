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

from .forms import LoginForm
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
    DDDQuantity
)


class IndexView(TemplateView):
    template_name = "index.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context

class LoginView(AuthLoginView):
    template_name = 'login.html'
    form_class = LoginForm

@method_decorator(login_required, name='dispatch')
class AnalyseView(TemplateView):
    template_name = "analyse.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
      
        all_orgs = Organisation.objects.all().order_by('ods_name')
        predecessor_map = {}
        org_list = []

        for org in all_orgs:
            org_name = org.ods_name
            org_list.append(org_name)
            
            if org.successor:
                successor_name = org.successor.ods_name
                if successor_name not in predecessor_map:
                    predecessor_map[successor_name] = []
                predecessor_map[successor_name].append(org_name)

        context['org_data'] = json.dumps({
            'items': org_list,
            'predecessorMap': predecessor_map
        }, default=str)
        
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
                "included": len(available_orgs),  # Updated to use total available orgs including predecessors
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

@login_required
@csrf_protect
@api_view(["POST"])
def filtered_quantities(request):
    search_items = request.data.get("names", None)
    ods_names = request.data.get("ods_names", None)
    quantity_type = request.data.get("quantity_type", None)
    
    if not all([search_items, ods_names, quantity_type]) or quantity_type == '--':
        return Response({"error": "Missing required parameters"}, status=400)


    vmp_ids = set()
    query = Q()
    for item in search_items:
        try:
            code, item_type = item.split('|')
            if item_type == 'vmp':
                query |= Q(code=code)
            elif item_type == 'vtm':
                query |= Q(vtm__vtm=code)
            elif item_type == 'ingredient':
                query |= Q(ingredients__code=code)
        except ValueError:
            return Response({"error": f"Invalid search item format: {item}"}, status=400)
    
    vmp_ids = set(VMP.objects.filter(query).values_list('id', flat=True))
    
    if not vmp_ids:
        return Response({"error": "No valid VMPs found"}, status=400)

    try:
        base_vmps = VMP.objects.filter(
            id__in=vmp_ids
        ).select_related('vtm').annotate(
            route_list=ArrayAgg('routes__name', distinct=True),
            ingredient_names=ArrayAgg('ingredients__name', distinct=True),
            ingredient_codes=ArrayAgg('ingredients__code', distinct=True)
        ).values(
            'id', 'code', 'name', 'vtm__name',
            'route_list', 'ingredient_names', 'ingredient_codes'
        )

        response_data = []
        for vmp in base_vmps:
            response_item = {
                'vmp__code': vmp['code'],
                'vmp__name': vmp['name'],
                'vmp__vtm__name': vmp['vtm__name'],
                'routes': [route for route in vmp['route_list'] if route],
                'ingredient_names': vmp['ingredient_names'],
                'ingredient_codes': vmp['ingredient_codes'],
                'organisation__ods_code': None,
                'organisation__ods_name': None,
                'organisation__region': None,
                'organisation__icb': None,
                'data': []
            }
            response_data.append(response_item)

        quantity_model = {
            "VMP Quantity": SCMDQuantity,
            "Ingredient Quantity": IngredientQuantity,
            "DDD": DDDQuantity
        }.get(quantity_type)

        if quantity_model:
            quantity_data = quantity_model.objects.filter(
                vmp_id__in=vmp_ids,
                organisation__ods_name__in=ods_names
            ).select_related('organisation')

            for item in quantity_data:
                response_item = {
                    'vmp__code': item.vmp.code,
                    'vmp__name': item.vmp.name,
                    'vmp__vtm__name': item.vmp.vtm.name if item.vmp.vtm else None,
                    'routes': [route.name for route in item.vmp.routes.all()],
                    'ingredient_names': [ing.name for ing in item.vmp.ingredients.all()],
                    'ingredient_codes': [ing.code for ing in item.vmp.ingredients.all()],
                    'organisation__ods_code': item.organisation.ods_code,
                    'organisation__ods_name': item.organisation.ods_name,
                    'organisation__region': item.organisation.region,
                    'organisation__icb': item.organisation.icb,
                    'data': item.data
                }
                response_data.append(response_item)

        return Response(response_data)

    except Exception as e:
        print(f"Error processing request: {str(e)}")
        return Response({"error": "An error occurred while processing the request"}, status=500)

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
    
    # Initialize empty sets for codes and display names
    vmp_codes = set()
    display_names = {}

    print(search_items)
    # Process each search item
    for item in search_items:
        code, item_type = item.split('|')
        
        if item_type == 'vtm':
            # Get all VMPs for this VTM
            vtm_vmps = VMP.objects.filter(vtm__vtm=code).values_list('code', flat=True)
            vmp_codes.update(vtm_vmps)
            # Get VTM display name
            vtm = VTM.objects.filter(vtm=code).first()
            if vtm:
                display_names[f"{code}|vtm"] = f"{vtm.name} ({code})"

        elif item_type == 'vmp':
            vmp_codes.add(code)
            # Get VMP display name
            vmp = VMP.objects.filter(code=code).first()
            if vmp:
                display_names[f"{code}|vmp"] = f"{vmp.name} ({code})"

        elif item_type == 'ingredient':
            # Get all VMPs containing this ingredient
            ingredient_vmps = VMP.objects.filter(ingredients__code=code).values_list('code', flat=True)
            vmp_codes.update(ingredient_vmps)
            # Get ingredient display name
            ingredient = Ingredient.objects.filter(code=code).first()
            if ingredient:
                display_names[f"{code}|ingredient"] = f"{ingredient.name} ({code})"

    # Count unique VMPs
    vmp_count = len(vmp_codes)
    
    return Response({
        "vmp_count": vmp_count,
        "display_names": display_names
    })


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

@method_decorator(login_required, name='dispatch')
class ProductDetailsView(TemplateView):
    template_name = "product_details.html"
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        search_term = self.request.GET.get('search', '')
        page = self.request.GET.get('page', 1)
        try:
            page = int(page)
        except ValueError:
            page = 1
        
        per_page = 50
        offset = (page - 1) * per_page
        
        vmps = VMP.objects.select_related('vtm').prefetch_related(
            'ingredients',
            'ddds',
            'routes'
        )
        
        if search_term:
            vmps = vmps.filter(
                Q(name__icontains=search_term) |
                Q(code__icontains=search_term) |
                Q(vtm__name__icontains=search_term) |
                Q(vtm__vtm__icontains=search_term)
            )
        
        vmps = vmps.order_by('name')
  
        total_count = vmps.count()
        total_pages = math.ceil(total_count / per_page)
        
        vmps = vmps[offset:offset + per_page]
        
        products = []
        for vmp in vmps:

            ingredient_names = ", ".join([i.name for i in vmp.ingredients.all()])

            ddd_value = None
            ddd = vmp.ddds.first()
            if ddd:
                ddd_value = {
                    'ddd': ddd.ddd,
                    'unit_type': ddd.unit_type,
                    'route': ddd.route.name
                }

            valid_quantity = SCMDQuantity.objects.filter(
                vmp=vmp,
                data__0__0__isnull=False,
                data__0__1__isnull=False,
                data__0__2__isnull=False
            ).select_related('organisation').first()

            example_quantity = None
            example_ingredient = None
            example_ddd = None
            example_month = None

            if valid_quantity and valid_quantity.data:
                example_month = valid_quantity.data[0][0]

                example_quantity = {
                    'quantity': valid_quantity.data[0][1],
                    'unit': valid_quantity.data[0][2]
                }

                ingredient_quantities = IngredientQuantity.objects.filter(
                    vmp=vmp,
                    organisation=valid_quantity.organisation,
                    data__0__0=example_month
                ).select_related('ingredient')

                example_ingredients = []
                if ingredient_quantities:
                    for ing_quantity in ingredient_quantities:
                        if ing_quantity.data:
                            example_ingredients.append({
                                'quantity': ing_quantity.data[0][1],
                                'unit': ing_quantity.data[0][2],
                                'name': ing_quantity.ingredient.name
                            })

                ddd_quantity = DDDQuantity.objects.filter(
                    vmp=vmp,
                    organisation=valid_quantity.organisation,
                    data__0__0=example_month
                ).first()

                if ddd_quantity and ddd_quantity.data:
                    example_ddd = {
                        'quantity': ddd_quantity.data[0][1],
                        'unit': ddd_quantity.data[0][2],
                        'ddd': ddd_value['ddd'] if ddd_value else None
                    }

            products.append({
                'vmp_name': vmp.name,
                'vmp_code': vmp.code,
                'vtm_name': vmp.vtm.name if vmp.vtm else None,
                'vtm_code': vmp.vtm.vtm if vmp.vtm else None,
                'routes': [route.name for route in vmp.routes.all()],
                'ingredient_names': ingredient_names,
                'ddd_value': ddd_value,
                'example_quantity': example_quantity,
                'example_ingredient': example_ingredient,
                'example_ddd': example_ddd,
                'example_ingredients': example_ingredients
            })
        
        context.update({
            'products': mark_safe(json.dumps(products, cls=DjangoJSONEncoder)),
            'current_page': str(page),
            'total_pages': str(total_pages),
            'search_term': search_term
        })

        return context

