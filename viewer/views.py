from django.shortcuts import render
from django.views.generic import TemplateView
from rest_framework import viewsets
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Dose, Organisation
from .serializers import DoseSerializer
import json

# Create your views here.
class IndexView(TemplateView):
    template_name = "index.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        doses = Dose.objects.all()
        dummy_data = DoseSerializer(doses, many=True).data
        context['dummy_data'] = json.dumps(dummy_data)
        print("Dummy data:", context['dummy_data'])  # Add this line
        return context

class DoseViewSet(viewsets.ModelViewSet):
    queryset = Dose.objects.all()
    serializer_class = DoseSerializer

@api_view(['GET'])
def unique_vmp_names(request):
    vmp_names = Dose.objects.values_list('vmp_name', flat=True).distinct().order_by('vmp_name')
    return Response(list(vmp_names))

@api_view(['GET'])
def unique_ods_names(request):
    ods_names = Organisation.objects.values_list('ods_name', flat=True).distinct().order_by('ods_name')
    return Response(list(ods_names))

@api_view(['POST'])
def filtered_doses(request):
    vmp_names = request.data.get('vmp_names', [])
    ods_names = request.data.get('ods_names', [])
    
    queryset = Dose.objects.all()
    
    if vmp_names:
        queryset = queryset.filter(vmp_name__in=vmp_names)
    
    if ods_names:
        queryset = queryset.filter(organisation__ods_name__in=ods_names)
    
    serializer = DoseSerializer(queryset, many=True)
    return Response(serializer.data)
