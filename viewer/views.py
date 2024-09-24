import json

from django.views.generic import TemplateView
from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.db.models import Prefetch
from django.db.models.functions import Coalesce
from django.db.models import F
from django.db.models import Value

from .models import (
    Dose,
    Organisation,
    VMP,
    IngredientQuantity,
    Ingredient,
    VTM,
    Measure,
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
            context["measure_result"] = json.dumps(values, ensure_ascii=False)

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
