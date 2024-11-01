from django.urls import path
from django.contrib.auth import views as auth_views
from .views import (
    filtered_vmp_count,
    IndexView,
    filtered_doses,
    filtered_ingredient_quantities,
    get_search_items,
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
    path("api/get-search-items/", get_search_items, name="get_search_items"),
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
