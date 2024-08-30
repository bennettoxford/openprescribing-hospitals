from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import DoseViewSet, unique_vmp_names, IndexView

router = DefaultRouter()
router.register(r'doses', DoseViewSet)

urlpatterns = [
    path('', IndexView.as_view(), name='index'),
    path('api/', include(router.urls)),
    path('api/unique-vmp-names/', unique_vmp_names, name='unique-vmp-names'),
]