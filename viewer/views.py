import json
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
from django.db.models import F
from django.db.models import Value
from django.utils.safestring import mark_safe
from django.db.models import Exists, OuterRef
from django.db.models import Count, Q, Prefetch

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
)


class IndexView(TemplateView):
    template_name = "index.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context


class AnalyseView(TemplateView):
    template_name = "analyse.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        return context


class MeasuresListView(TemplateView):
    template_name = "measures_list.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        measures = Measure.objects.select_related('reason').filter(draft=False)
        
        markdowner = Markdown()
        for measure in measures:
            measure.why_it_matters = markdowner.convert(measure.why_it_matters)
        
        context["measures"] = measures
        return context


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
        return {
            "measure_name": measure.name,
            "measure_name_short": measure.short_name,
            "why_it_matters": markdowner.convert(measure.why_it_matters),
            "reason": measure.reason.reason if measure.reason else None,
            "reason_colour": measure.reason.colour if measure.reason else None,
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
                            .annotate(non_zero_count=Count('id', filter=Q(quantity__isnull=False) & ~Q(quantity=0)))
                            .filter(non_zero_count__gt=0)
                            .values_list('organisation__ods_code', flat=True))

        org_measures_dict = {}
        for measure in org_measures.filter(organisation__ods_code__in=non_zero_orgs).values(
            'organisation__ods_code', 'organisation__ods_name', 'month', 'quantity'
        ):
            org_key = f"{measure['organisation__ods_code']} | {measure['organisation__ods_name']}"
            org_measures_dict.setdefault(org_key, []).append({
                'month': measure['month'],
                'quantity': measure['quantity']
            })

        return {
            "orgs_included": {
                "included": len(non_zero_orgs),
                "total": len(all_orgs)
            },
            "org_data": json.dumps(org_measures_dict, cls=DjangoJSONEncoder),
        }

    def get_aggregated_data(self, aggregated_measures):
        region_data = {}
        icb_data = {}

        for measure in aggregated_measures:
            data = region_data if measure.category == 'region' else icb_data
            data.setdefault(measure.label, {
                'name': measure.label,
                'data': []
            })['data'].append({
                'month': measure.month,
                'quantity': measure.quantity
            })

        region_list = list(region_data.values())
        icb_list = list(icb_data.values())

        return {
            "region_data": json.dumps(region_list, cls=DjangoJSONEncoder),
            "icb_data": json.dumps(icb_list, cls=DjangoJSONEncoder),
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


@api_view(["GET"])
def unique_vmp_names(request):
    vmp_data = VMP.objects.values('name', 'code').distinct().order_by('name')
    formatted_vmp = [f"{item['code']} | {item['name']}" for item in vmp_data]
    return Response(formatted_vmp)


@api_view(["GET"])
def unique_ods_names(request):
    ods_data = Organisation.objects.values('ods_name', 'ods_code').distinct().order_by('ods_name')
    formatted_ods = [f"{item['ods_code']} | {item['ods_name']}" for item in ods_data]
    return Response(formatted_ods)


@api_view(["GET"])
def unique_ingredient_names(request):
    ingredient_data = Ingredient.objects.values('name', 'code').distinct().order_by('name')
    formatted_ingredient = [f"{item['code']} | {item['name']}" for item in ingredient_data]
    return Response(formatted_ingredient)


@api_view(["GET"])
def unique_vtm_names(request):
    vtm_data = VTM.objects.values('vtm', 'name').distinct().order_by('name')
    formatted_vtm = [f"{item['vtm']} | {item['name']}" for item in vtm_data]
    return Response(formatted_vtm)


@api_view(["GET"])
def unique_atc_codes(request):
    # Subquery to check if an ATC code has associated VMPs
    vmp_exists = VMP.objects.filter(atcs__code=OuterRef('code')).values('code')

    atc_data = ATC.objects.annotate(
        has_vmps=Exists(vmp_exists)
    ).values('code', 'name', 'has_vmps').distinct().order_by('code')
    
    atc_hierarchy = {}
    
    for item in atc_data:
        code = item['code']
        name = item['name']
        has_vmps = item['has_vmps']
        formatted_item = f"{code} | {name}"
        
        atc_hierarchy[code] = {
            'name': formatted_item,
            'children': [],
            'has_vmps': has_vmps
        }
     
        for i in range(1, len(code)):
            parent_code = code[:i]
            if parent_code in atc_hierarchy:
                atc_hierarchy[parent_code]['children'].append(code)
                # If a child has VMPs, the parent should be marked as having VMPs too
                if has_vmps:
                    atc_hierarchy[parent_code]['has_vmps'] = True
 
    formatted_atc = [
        {
            'code': code,
            'name': data['name'],
            'children': data['children'],
            'has_vmps': data['has_vmps']
        }
        for code, data in atc_hierarchy.items()
    ]
    
    return JsonResponse(formatted_atc, safe=False)


@api_view(["POST"])
def filtered_doses(request):
    search_items = request.data.get("names", [])
    ods_names = request.data.get("ods_names", [])
    search_type = request.data.get("search_type", "vmp")

    search_items = [item.split("|")[0].strip() for item in search_items]
    if search_type == "vmp":
        queryset = Dose.objects.filter(vmp__code__in=search_items)
    elif search_type == "vtm":
        queryset = Dose.objects.filter(vmp__vtm__vtm__in=search_items)
    elif search_type == "ingredient":
        queryset = Dose.objects.filter(vmp__ingredients__code__in=search_items)
    elif search_type == "atc":
        # Get all children of the selected ATC codes
        all_atc_codes = get_all_child_atc_codes(search_items)
        queryset = Dose.objects.filter(vmp__atcs__code__in=all_atc_codes)
    
    if ods_names:
        ods_names = [item.split("|")[0].strip() for item in ods_names]
        queryset = queryset.filter(organisation__ods_code__in=ods_names)

    queryset = (
        queryset.select_related("vmp", "organisation", "vmp__vtm")
        .prefetch_related(
            Prefetch(
                "vmp__ingredients",
                queryset=Ingredient.objects.only("name"),
                to_attr="prefetched_ingredients",
            ),
            "vmp__atcs"
        )
        .order_by("year_month", "vmp__name", "organisation__ods_name")
    )

    data = list(
        queryset.values(
            "id",
            "year_month",
            "quantity",
            "unit",
            vmp_code=F("vmp__code"),
            vmp_name=F("vmp__name"),
            ods_code=F("organisation__ods_code"),
            ods_name=F("organisation__ods_name"),
            vtm_name=Coalesce("vmp__vtm__name", Value("")),
        )
    )

    # Add ingredient names and ATC information to the data
    vmp_ingredient_map = {
        dose.id: [ing.name for ing in dose.vmp.prefetched_ingredients]
        for dose in queryset
    }
    vmp_atc_map = {
        dose.id: [{'code': atc.code, 'name': atc.name} for atc in dose.vmp.atcs.all()]
        for dose in queryset
    }
    for item in data:
        item["ingredient_names"] = vmp_ingredient_map[item["id"]]
        atc_info = vmp_atc_map[item["id"]]
        item["atc_code"] = atc_info[0]['code'] if atc_info else ""
        item["atc_name"] = atc_info[0]['name'] if atc_info else "Unknown ATC"

    return Response(data)


@api_view(["POST"])
def filtered_ingredient_quantities(request):
    search_items = request.data.get("names", [])
    ods_names = request.data.get("ods_names", [])
    search_type = request.data.get("search_type", "")

    queryset = IngredientQuantity.objects.all()

    search_items = [item.split("|")[0].strip() for item in search_items]

    if search_type == "vmp":
        queryset = queryset.filter(vmp__code__in=search_items)
    elif search_type == "vtm":
        queryset = queryset.filter(vmp__vtm__vtm__in=search_items)
    elif search_type == "ingredient":
        queryset = queryset.filter(ingredient__name__in=search_items)
    elif search_type == "atc":
        all_atc_codes = get_all_child_atc_codes(search_items)
        queryset = queryset.filter(vmp__atcs__code__in=all_atc_codes)

    if ods_names:
        ods_names = [item.split("|")[0].strip() for item in ods_names]
        queryset = queryset.filter(organisation__ods_code__in=ods_names)

    queryset = (
        queryset.select_related("vmp", "organisation", "vmp__vtm", "ingredient")
        .prefetch_related("vmp__atcs")
        .order_by("year_month", "vmp__name", "organisation__ods_name", "ingredient__name")
    )

    data = list(
        queryset.values(
            "id",
            "year_month",
            "quantity",
            "unit",
            ingredient_code=F("ingredient__code"),
            ingredient_name=F("ingredient__name"),
            vmp_code=F("vmp__code"),
            vmp_name=F("vmp__name"),
            ods_code=F("organisation__ods_code"),
            ods_name=F("organisation__ods_name"),
            vtm_name=Coalesce("vmp__vtm__name", Value("")),
        )
    )

    # Add ATC information to the data
    vmp_atc_map = {
        iq.id: [{'code': atc.code, 'name': atc.name} for atc in iq.vmp.atcs.all()]
        for iq in queryset
    }
    for item in data:
        atc_info = vmp_atc_map[item["id"]]
        item["atc_code"] = atc_info[0]['code'] if atc_info else ""
        item["atc_name"] = atc_info[0]['name'] if atc_info else "Unknown ATC"

    return Response(data)


class OrgsSubmittingDataView(TemplateView):
    template_name = 'org_submissions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Step 1: Collect data
        org_data = defaultdict(lambda: {'successor': None, 'submissions': {}, 'predecessors': []})
        all_dates = set()
        for cache in OrgSubmissionCache.objects.select_related('organisation', 'successor').order_by('organisation__ods_name', 'month'):
            org_name = cache.organisation.ods_name
            org_data[org_name]['successor'] = cache.successor.ods_name if cache.successor else None
            month_str = cache.month.isoformat() if isinstance(cache.month, date) else str(cache.month)
            org_data[org_name]['submissions'][month_str] = cache.has_submitted
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


@api_view(["POST"])
def filtered_vmp_count(request):
    search_items = request.data.get("names", [])
    search_type = request.data.get("search_type", "vmp")

    search_items = [item.split("|")[0].strip() for item in search_items]
    
    if search_type == "vmp":
        queryset = VMP.objects.filter(code__in=search_items)
    elif search_type == "vtm":
        queryset = VMP.objects.filter(vtm__vtm__in=search_items)
    elif search_type == "ingredient":
        queryset = VMP.objects.filter(ingredients__code__in=search_items)
    elif search_type == "atc":
        all_atc_codes = get_all_child_atc_codes(search_items)
        queryset = VMP.objects.filter(atcs__code__in=all_atc_codes)
    
    vmp_count = queryset.distinct().count()
    return Response({"vmp_count": vmp_count})
