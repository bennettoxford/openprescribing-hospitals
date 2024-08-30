from rest_framework import serializers
from .models import Dose, Organisation, VMP, SCMD, IngredientQuantity

class OrganisationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Organisation
        fields = ['ods_code', 'ods_name', 'region']

class VMPSerializer(serializers.ModelSerializer):
    class Meta:
        model = VMP
        fields = ['code', 'name', 'vtm']

class DoseSerializer(serializers.ModelSerializer):
    vmp_code = serializers.CharField(source='vmp.code', read_only=True)
    vmp_name = serializers.CharField(source='vmp.name', read_only=True)
    ods_code = serializers.CharField(source='organisation.ods_code', read_only=True)
    ods_name = serializers.CharField(source='organisation.ods_name', read_only=True)

    class Meta:
        model = Dose
        fields = ['id', 'year_month', 'vmp_code', 'vmp_name', 'ods_code', 'ods_name', 'quantity', 'unit']

class SCMDSerializer(serializers.ModelSerializer):
    vmp_code = serializers.CharField(source='vmp.code', read_only=True)
    vmp_name = serializers.CharField(source='vmp.name', read_only=True)
    ods_code = serializers.CharField(source='organisation.ods_code', read_only=True)
    ods_name = serializers.CharField(source='organisation.ods_name', read_only=True)

    class Meta:
        model = SCMD
        fields = ['id', 'year_month', 'vmp_code', 'vmp_name', 'ods_code', 'ods_name', 'quantity', 'unit']

class IngredientQuantitySerializer(serializers.ModelSerializer):
    ingredient_code = serializers.CharField(source='ingredient.code', read_only=True)
    ingredient_name = serializers.CharField(source='ingredient.name', read_only=True)
    vmp_code = serializers.CharField(source='vmp.code', read_only=True)
    vmp_name = serializers.CharField(source='vmp.name', read_only=True)
    ods_code = serializers.CharField(source='organisation.ods_code', read_only=True)
    ods_name = serializers.CharField(source='organisation.ods_name', read_only=True)

    class Meta:
        model = IngredientQuantity
        fields = ['id', 'year_month', 'ingredient_code', 'ingredient_name', 'vmp_code', 'vmp_name', 'ods_code', 'ods_name', 'quantity', 'unit']