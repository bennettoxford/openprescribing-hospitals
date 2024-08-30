from django.shortcuts import render
from django.views.generic import TemplateView
from rest_framework import viewsets
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Dose
from .serializers import DoseSerializer

# Create your views here.
class IndexView(TemplateView):
    template_name = "index.html"

class DoseViewSet(viewsets.ModelViewSet):
    queryset = Dose.objects.all()
    serializer_class = DoseSerializer

@api_view(['GET'])
def unique_vmp_names(request):
    vmp_names = Dose.objects.values_list('vmp_name', flat=True).distinct().order_by('vmp_name')
    return Response(list(vmp_names))
