from django.contrib import admin
from .models import (
    VTM,
    VMP,
    Ingredient,
    Organisation,
    Dose,
    IngredientQuantity,
    Measure,
    MeasureReason,
    Route,
)

# Register your models here.


@admin.register(VTM)
class VTMAdmin(admin.ModelAdmin):
    list_display = ("vtm", "name")
    search_fields = ("vtm", "name")


@admin.register(VMP)
class VMPAdmin(admin.ModelAdmin):
    list_display = ("code", "name", "vtm")
    search_fields = ("code", "name")
    list_filter = ("vtm",)


@admin.register(Ingredient)
class IngredientAdmin(admin.ModelAdmin):
    list_display = ("code", "name")
    search_fields = ("code", "name")


@admin.register(Organisation)
class OrganisationAdmin(admin.ModelAdmin):
    list_display = ("ods_code", "ods_name", "region", "successor")
    search_fields = ("ods_code", "ods_name", "region")
    list_filter = ("region",)


@admin.register(Dose)
class DoseAdmin(admin.ModelAdmin):
    list_display = ("year_month", "vmp", "quantity", "unit", "organisation")
    list_filter = ("year_month", "unit", "organisation")
    search_fields = ("vmp__name", "organisation__ods_name")


@admin.register(IngredientQuantity)
class IngredientQuantityAdmin(admin.ModelAdmin):
    list_display = (
        "year_month",
        "ingredient",
        "vmp",
        "quantity",
        "unit",
        "organisation",
    )
    list_filter = ("year_month", "unit", "organisation")
    search_fields = ("ingredient__name", "vmp__name", "organisation__ods_name")


@admin.register(Measure)
class MeasureAdmin(admin.ModelAdmin):
    list_display = ("name", "reason")
    search_fields = ("name", "reason")


@admin.register(MeasureReason)
class MeasureReasonAdmin(admin.ModelAdmin):
    list_display = ("reason", "colour")
    search_fields = ("reason", "colour")


@admin.register(Route)
class RouteAdmin(admin.ModelAdmin):
    list_display = ("code", "name")
    search_fields = ("code", "name")
