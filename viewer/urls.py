from django.urls import path
from django.contrib.auth import views as auth_views
from .views import (
    IndexView,
    AnalyseView,
    ContactView,
    FAQView,
    BlogListView,
    PapersListView,
    ProductLookupView,
    AboutView,
    MeasuresListView,
    MeasureItemView,
    vmp_count,
    get_quantity_data,
    get_product_details,
    search_products,
    LoginView,
    SubmissionHistoryView,
)

app_name = "viewer"

urlpatterns = [
    path("", IndexView.as_view(), name="index"),
    path("analyse/", AnalyseView.as_view(), name="analyse"),
    path("measures/", MeasuresListView.as_view(), name="measures_list"),
    path("measures/<slug:slug>/", MeasureItemView.as_view(), name="measure_item"),
    path("submission-history/", SubmissionHistoryView.as_view(), name="submission_history"),
    path('login/', LoginView.as_view(), name='login'),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),
    path('contact/', ContactView.as_view(), name='contact'),
    path('faq/', FAQView.as_view(), name='faq'),
    path('product-lookup/', ProductLookupView.as_view(), name='product_lookup'),
    path('about/', AboutView.as_view(), name='about'),
    path('blog/', BlogListView.as_view(), name='blog_list'),
    path('research/', PapersListView.as_view(), name='papers_list'),
    path("api/vmp-count/", vmp_count, name="vmp_count"),
    path("api/get-quantity-data/", get_quantity_data, name="get_quantity_data"),
    path("api/get-product-details/", get_product_details, name="get_product_details"),
    path("api/search-products/", search_products, name="search_products"),
]
