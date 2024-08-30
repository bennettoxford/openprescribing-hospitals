from django.shortcuts import render
from django.views.generic import TemplateView
from datetime import datetime, timedelta
from .models import Dose
import json

# Create your views here.
class IndexView(TemplateView):
    template_name = "index.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
    
        # Generate dummy data for 6 months and 3 ODS codes
        start_date = datetime(2020, 11, 1)
        ods_codes = [
            {'code': 'RK9', 'name': 'University Hospitals Plymouth NHS Trust'},
            {'code': 'RT2', 'name': 'Pennine Care NHS Foundation Trust'},
            {'code': 'RXY', 'name': 'Kent And Medway NHS And Social Care Partnership Trust'}
        ]
        medications = [
            {
                'vmp_code': '42206511000001102',
                'vmp_name': 'Apixaban 5mg tablets',
                'base_quantity': 1000
            },
            {
                'vmp_code': '14254711000001104',
                'vmp_name': 'Rivaroxaban 10mg tablets',
                'base_quantity': 38
            }
        ]
        
        dummy_data = []
        for i in range(6):
            current_date = start_date + timedelta(days=30*i)
            for ods in ods_codes:
                for med in medications:
                    quantity = med['base_quantity'] + (i * 10) + (50 if ods['code'] == 'RK9' else 0)
                    dose = {
                        'year_month': current_date.strftime('%Y-%m-%d'),
                        'vmp_code': med['vmp_code'],
                        'vmp_name': med['vmp_name'],
                        'ods_code': ods['code'],
                        'ods_name': ods['name'],
                        'SCMD_quantity': quantity,
                        'SCMD_quantity_basis': 'tablet',
                        'dose_quantity': quantity,
                        'converted_udfs': 1.0,
                        'udfs_basis': 'tablet',
                        'dose_unit': 'tablet',
                        'df_ind': 'Discrete'
                    }
                    dummy_data.append(dose)
        
        # Convert data to JSON for safe passing to template
        context['dummy_data'] = json.dumps(dummy_data)
    
        return context
