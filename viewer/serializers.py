from rest_framework import serializers
from .models import Dose, Organisation

class OrganisationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Organisation
        fields = ['id', 'ods_code', 'ods_name', 'region']

class DoseSerializer(serializers.ModelSerializer):
    ods_code = serializers.CharField(source='organisation.ods_code', read_only=True)
    ods_name = serializers.CharField(source='organisation.ods_name', read_only=True)

    class Meta:
        model = Dose
        fields = ['id', 'year_month', 'vmp_code', 'vmp_name', 'ods_code', 'ods_name', 'SCMD_quantity',
                  'SCMD_quantity_basis', 'dose_quantity', 'converted_udfs', 'udfs_basis', 'dose_unit', 'df_ind']