from django.contrib import admin
from .models import (
    VTM,
    VMP,
    Ingredient,
    Organisation,
    Dose,
    IngredientQuantity,
    Measure,
    MeasureTag,
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
    list_display = ('vmp', 'organisation', 'get_latest_quantity', 'get_latest_unit')
    list_filter = ('vmp', 'organisation')
    search_fields = ('vmp__name', 'organisation__ods_name')

    def get_latest_quantity(self, obj):
        if obj.data and len(obj.data) > 0:
            return obj.data[-1][1]
        return None
    get_latest_quantity.short_description = 'Latest Quantity'

    def get_latest_unit(self, obj):
        if obj.data and len(obj.data) > 0:
            return obj.data[-1][2]
        return None
    get_latest_unit.short_description = 'Latest Unit'


@admin.register(IngredientQuantity)
class IngredientQuantityAdmin(admin.ModelAdmin):
    list_display = ('ingredient', 'vmp', 'organisation', 'get_latest_quantity', 'get_latest_unit')
    list_filter = ('ingredient', 'vmp', 'organisation')
    search_fields = ('ingredient__name', 'vmp__name', 'organisation__ods_name')

    def get_latest_quantity(self, obj):
        if obj.data and len(obj.data) > 0:
            return obj.data[-1][1]
        return None
    get_latest_quantity.short_description = 'Latest Quantity'

    def get_latest_unit(self, obj):
        if obj.data and len(obj.data) > 0:
            return obj.data[-1][2]
        return None
    get_latest_unit.short_description = 'Latest Unit'


@admin.register(Measure)
class MeasureAdmin(admin.ModelAdmin):
    list_display = ("name", "draft")
    search_fields = ("name", "tags")


@admin.register(MeasureTag)
class MeasureTagAdmin(admin.ModelAdmin):
    list_display = ("name", "colour")
    search_fields = ("name", "colour")


@admin.register(Route)
class RouteAdmin(admin.ModelAdmin):
    list_display = ("code", "name")
    search_fields = ("code", "name")
