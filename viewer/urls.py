from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import DoseViewSet, unique_vmp_names, unique_ods_names, IndexView, filtered_doses, unique_ingredient_names, unique_vtm_names

router = DefaultRouter()
router.register(r'doses', DoseViewSet)

app_name = 'viewer'

urlpatterns = [
    path('', IndexView.as_view(), name='index'),
    path('api/', include(router.urls)),
    path('api/unique-vmp-names/', unique_vmp_names, name='unique_vmp_names'),
    path('api/unique-ods-names/', unique_ods_names, name='unique_ods_names'),
    path('api/unique-ingredient-names/', unique_ingredient_names, name='unique_ingredient_names'),
    path('api/unique-vtm-names/', unique_vtm_names, name='unique_vtm_names'),
    path('api/filtered-doses/', filtered_doses, name='filtered-doses'),
]