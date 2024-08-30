from django.shortcuts import render
from django.views.generic import TemplateView
from rest_framework import viewsets
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Dose, Organisation, VMP, SCMD, IngredientQuantity, Ingredient, VTM
from .serializers import DoseSerializer, OrganisationSerializer, VMPSerializer, SCMDSerializer, IngredientQuantitySerializer
import json

# Create your views here.
class IndexView(TemplateView):
    template_name = "index.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        doses = Dose.objects.all()
        dummy_data = DoseSerializer(doses, many=True).data
        context['dummy_data'] = json.dumps(dummy_data)
        return context

class DoseViewSet(viewsets.ModelViewSet):
    queryset = Dose.objects.all()
    serializer_class = DoseSerializer

class SCMDViewSet(viewsets.ModelViewSet):
    queryset = SCMD.objects.all()
    serializer_class = SCMDSerializer

class IngredientQuantityViewSet(viewsets.ModelViewSet):
    queryset = IngredientQuantity.objects.all()
    serializer_class = IngredientQuantitySerializer

@api_view(['GET'])
def unique_vmp_names(request):
    vmp_names = VMP.objects.values_list('name', flat=True).distinct().order_by('name')
    return Response(list(vmp_names))

@api_view(['GET'])
def unique_ods_names(request):
    ods_names = Organisation.objects.values_list('ods_name', flat=True).distinct().order_by('ods_name')
    return Response(list(ods_names))

@api_view(['GET'])
def unique_ingredient_names(request):
    ingredient_names = Ingredient.objects.values_list('name', flat=True).distinct().order_by('name')
    return Response(list(ingredient_names))

@api_view(['GET'])
def unique_vtm_names(request):
    vtm_names = VTM.objects.values_list('name', flat=True).distinct().order_by('name')
    return Response(list(vtm_names))

@api_view(['POST'])
def filtered_doses(request):
    vmp_names = request.data.get('vmp_names', [])
    ods_names = request.data.get('ods_names', [])
    
    queryset = Dose.objects.all()
    
    if vmp_names:
        queryset = queryset.filter(vmp__name__in=vmp_names)
    
    if ods_names:
        queryset = queryset.filter(organisation__ods_name__in=ods_names)
    

    serializer = DoseSerializer(queryset, many=True)
    print(serializer.data)
    return Response(serializer.data)

@api_view(['POST'])
def filtered_scmds(request):
    vmp_names = request.data.get('vmp_names', [])
    ods_names = request.data.get('ods_names', [])
    
    queryset = SCMD.objects.all()
    
    if vmp_names:
        queryset = queryset.filter(vmp__name__in=vmp_names)
    
    if ods_names:
        queryset = queryset.filter(organisation__ods_name__in=ods_names)
    
    serializer = SCMDSerializer(queryset, many=True)
    return Response(serializer.data)

@api_view(['POST'])
def filtered_ingredient_quantities(request):
    ingredient_names = request.data.get('ingredient_names', [])
    vmp_names = request.data.get('vmp_names', [])
    ods_names = request.data.get('ods_names', [])
    
    queryset = IngredientQuantity.objects.all()
    
    if ingredient_names:
        queryset = queryset.filter(ingredient__name__in=ingredient_names)
    
    if vmp_names:
        queryset = queryset.filter(vmp__name__in=vmp_names)
    
    if ods_names:
        queryset = queryset.filter(organisation__ods_name__in=ods_names)
    
    serializer = IngredientQuantitySerializer(queryset, many=True)
    return Response(serializer.data)
