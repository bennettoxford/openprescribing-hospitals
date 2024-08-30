from rest_framework import serializers
from .models import Dose

class DoseSerializer(serializers.ModelSerializer):
    class Meta:
        model = Dose
        fields = '__all__'