from django.urls import path
from django.contrib.auth import views as auth_views
from .views import (
    filtered_vmp_count,
    IndexView,
    filtered_quantities,
    AnalyseView,
    MeasuresListView,
    MeasureItemView,
    OrgsSubmittingDataView,
    LoginView,
    ContactView,
    FAQView,
    search_items,
)


app_name = "viewer"

urlpatterns = [
    path("", IndexView.as_view(), name="index"),
    path("analyse/", AnalyseView.as_view(), name="analyse"),
    path("measures/", MeasuresListView.as_view(), name="measures_list"),
    path("api/filtered-vmp-count/", filtered_vmp_count, name="filtered_vmp_count"),
    path("api/filtered-quantities/", filtered_quantities, name="filtered-quantities"),
    path("measures/<slug:slug>/", MeasureItemView.as_view(), name="measure_item"),
    path("submission-history/", OrgsSubmittingDataView.as_view(), name="submission_history"),
    path('login/', LoginView.as_view(), name='login'),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),
    path('contact/', ContactView.as_view(), name='contact'),
    path('faq/', FAQView.as_view(), name='faq'),
    path("api/search/", search_items, name="search_items"),
]
