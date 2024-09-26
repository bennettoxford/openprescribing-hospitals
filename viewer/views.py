import json
from collections import defaultdict
from datetime import date

from django.views.generic import TemplateView
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.db.models import Prefetch
from django.db.models.functions import Coalesce
from django.db.models import F
from django.db.models import Value
from django.utils.safestring import mark_safe

from .models import (
    Dose,
    Organisation,
    VMP,
    IngredientQuantity,
    Ingredient,
    VTM,
    Measure,
    OrgSubmissionCache,
)
from .measures.measure_utils import execute_measure_sql


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
        measures_by_category = {}
        for measure in Measure.objects.all():
            category = measure.category or 'Uncategorised'
            if category not in measures_by_category:
                measures_by_category[category] = []
            measures_by_category[category].append(measure)
        
        # Ensure "Experimental" category is last
        if "Experimental" in measures_by_category:
            experimental_measures = measures_by_category.pop("Experimental")
            measures_by_category["Experimental"] = experimental_measures
        
        context["measures_by_category"] = measures_by_category
        return context


class MeasureItemView(TemplateView):
    template_name = "measure_item.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        slug = self.kwargs.get("slug")
        measure = Measure.objects.get(slug=slug)

        context["measure_name"] = measure.name
        context["description"] = measure.description
        context["why"] = measure.why
        try:
            result = execute_measure_sql(measure.name)
            values = result.get("values", [])

            org_data = defaultdict(lambda: defaultdict(float))
            all_months = set()
            all_orgs = set()

            for row in values:
                month = row['month']
                org = row['organisation']
                value = row['quantity']
                org_data[org][month] = value
                all_months.add(month)
                all_orgs.add(org)

            all_months = sorted(all_months)

            # Fill in missing data with 0
            filled_values = []
            for org in all_orgs:
                for month in all_months:
                    filled_values.append({
                        'organisation': org,
                        'region': next((v['region'] for v in values if v['organisation'] == org), ''),
                        'month': month,
                        'quantity': org_data[org][month]
                    })

            context["measure_result"] = json.dumps(filled_values, ensure_ascii=False)

            results_by_month = defaultdict(list)
            for row in filled_values:
                month = row['month']
                value = row['quantity']
                results_by_month[month].append(value)

            percentiles = {}
            percentile_values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99]
            for month, values in results_by_month.items():
                sorted_values = sorted(values)
                percentiles[month] = [
                    sorted_values[int(len(sorted_values) * p / 100)] for p in percentile_values
                ]

            context["deciles"] = json.dumps(percentiles, ensure_ascii=False)

        except ValueError as e:
            context["error"] = str(e)
        except FileNotFoundError as e:
            context["error"] = str(e)
        except Exception as e:
            context["error"] = str(e)
        return context


@api_view(["GET"])
def unique_vmp_names(request):
    vmp_names = VMP.objects.values_list(
        "name", flat=True).distinct().order_by("name")
    return Response(list(vmp_names))


@api_view(["GET"])
def unique_ods_names(request):
    ods_names = (
        Organisation.objects.values_list("ods_name", flat=True)
        .distinct()
        .order_by("ods_name")
    )
    return Response(list(ods_names))


@api_view(["GET"])
def unique_ingredient_names(request):
    ingredient_names = (Ingredient.objects.values_list(
        "name", flat=True).distinct().order_by("name"))
    return Response(list(ingredient_names))


@api_view(["GET"])
def unique_vtm_names(request):
    vtm_names = VTM.objects.values_list(
        "name", flat=True).distinct().order_by("name")
    return Response(list(vtm_names))


@api_view(["POST"])
def filtered_doses(request):
    vmp_names = request.data.get("vmp_names", [])
    ods_names = request.data.get("ods_names", [])
    search_type = request.data.get("search_type", "vmp")

    if search_type == "vmp":
        queryset = Dose.objects.filter(vmp__name__in=vmp_names)
    elif search_type == "vtm":
        queryset = Dose.objects.filter(vmp__vtm__name__in=vmp_names)
    elif search_type == "ingredient":
        queryset = Dose.objects.filter(vmp__ingredients__name__in=vmp_names)

    if ods_names:
        queryset = queryset.filter(organisation__ods_name__in=ods_names)

    queryset = (
        queryset.select_related("vmp", "organisation", "vmp__vtm")
        .prefetch_related(
            Prefetch(
                "vmp__ingredients",
                queryset=Ingredient.objects.only("name"),
                to_attr="prefetched_ingredients",
            )
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

    # Add ingredient names to the data
    vmp_ingredient_map = {
        dose.id: [ing.name for ing in dose.vmp.prefetched_ingredients]
        for dose in queryset
    }
    for item in data:
        item["ingredient_names"] = vmp_ingredient_map[item["id"]]

    return Response(data)


@api_view(["POST"])
def filtered_ingredient_quantities(request):

    search_items = request.data.get("vmp_names", [])
    ods_names = request.data.get("ods_names", [])

    search_type = request.data.get("search_type", "")

    queryset = IngredientQuantity.objects.all()

    if search_type == "vmp":
        queryset = queryset.filter(vmp__name__in=search_items)
    elif search_type == "vtm":
        queryset = queryset.filter(vmp__vtm__name__in=search_items)
    elif search_type == "ingredient":
        queryset = queryset.filter(ingredient__name__in=search_items)

    if ods_names:
        queryset = queryset.filter(organisation__ods_name__in=ods_names)

    queryset = queryset.select_related(
        "vmp",
        "organisation",
        "vmp__vtm",
        "ingredient").order_by(
        "year_month",
        "vmp__name",
        "organisation__ods_name",
        "ingredient__name")

    data = queryset.values(
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

    return Response(list(data))


class OrgsSubmittingDataView(TemplateView):
    template_name = 'org_submissions.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        # Step 1: Collect data
        org_data = defaultdict(lambda: {'successor': None, 'submissions': {}, 'predecessors': []})
        for cache in OrgSubmissionCache.objects.select_related('organisation', 'successor').order_by('organisation__ods_name', 'month'):
            org_name = cache.organisation.ods_name
            org_data[org_name]['successor'] = cache.successor.ods_name if cache.successor else None
            month_str = cache.month.isoformat() if isinstance(cache.month, date) else str(cache.month)
            org_data[org_name]['submissions'][month_str] = cache.has_submitted

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
        return context