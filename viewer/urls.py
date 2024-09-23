from django.urls import path
from .views import unique_vmp_names, unique_ods_names, IndexView, filtered_doses, filtered_ingredient_quantities, unique_ingredient_names, unique_vtm_names, AnalyseView, MeasuresListView, MeasureItemView


app_name = 'viewer'

urlpatterns = [
    path('', IndexView.as_view(), name='index'),
    path('analyse/', AnalyseView.as_view(), name='analyse'),
    path('measures/', MeasuresListView.as_view(), name='measures_list'),
    path('api/unique-vmp-names/', unique_vmp_names, name='unique_vmp_names'),
    path('api/unique-ods-names/', unique_ods_names, name='unique_ods_names'),
    path('api/unique-ingredient-names/', unique_ingredient_names, name='unique_ingredient_names'),
    path('api/unique-vtm-names/', unique_vtm_names, name='unique_vtm_names'),
    path('api/filtered-doses/', filtered_doses, name='filtered-doses'),
    path('api/filtered-ingredient-quantities/', filtered_ingredient_quantities, name='filtered-ingredient-quantities'),
    path('measures/<int:pk>/', MeasureItemView.as_view(), name='measure_item'),
]