from django.urls import path
from django.contrib.auth import views as auth_views
from .views import (
    filtered_vmp_count,
    unique_vmp_names,
    unique_ods_names,
    unique_atc_codes,
    IndexView,
    filtered_doses,
    filtered_ingredient_quantities,
    unique_ingredient_names,
    unique_vtm_names,
    AnalyseView,
    MeasuresListView,
    MeasureItemView,
    OrgsSubmittingDataView,
    LoginView,
    ContactView,
)


app_name = "viewer"

urlpatterns = [
    path("", IndexView.as_view(), name="index"),
    path("analyse/", AnalyseView.as_view(), name="analyse"),
    path("measures/", MeasuresListView.as_view(), name="measures_list"),
    path("api/filtered-vmp-count/", filtered_vmp_count, name="filtered_vmp_count"),
    path("api/unique-vmp-names/", unique_vmp_names, name="unique_vmp_names"),
    path("api/unique-ods-names/", unique_ods_names, name="unique_ods_names"),
    path("api/unique-atc-codes/", unique_atc_codes, name="unique_atc_codes"),
    path(
        "api/unique-ingredient-names/",
        unique_ingredient_names,
        name="unique_ingredient_names",
    ),
    path("api/unique-vtm-names/", unique_vtm_names, name="unique_vtm_names"),
    path("api/filtered-doses/", filtered_doses, name="filtered-doses"),
    path(
        "api/filtered-ingredient-quantities/",
        filtered_ingredient_quantities,
        name="filtered-ingredient-quantities",
    ),
    path("measures/<slug:slug>/", MeasureItemView.as_view(), name="measure_item"),
    path("submission-history/", OrgsSubmittingDataView.as_view(), name="submission_history"),
    path('login/', LoginView.as_view(), name='login'),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),
    path('contact/', ContactView.as_view(), name='contact'),
]
