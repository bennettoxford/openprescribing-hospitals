from django.urls import path
from .views import unique_vmp_names, unique_ods_names, unique_ingredient_names, unique_vtm_names

urlpatterns = [
    # ... (existing URL patterns) ...
    path('api/unique-vmp-names/', unique_vmp_names, name='unique_vmp_names'),
    path('api/unique-ods-names/', unique_ods_names, name='unique_ods_names'),
    path('api/unique-ingredient-names/', unique_ingredient_names, name='unique_ingredient_names'),
    path('api/unique-vtm-names/', unique_vtm_names, name='unique_vtm_names'),
    # ... (other URL patterns) ...
]