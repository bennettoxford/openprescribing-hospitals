from django.shortcuts import render
from django.views.generic import TemplateView
from datetime import datetime, timedelta

# Create your views here.
class IndexView(TemplateView):
    template_name = "index.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
    
        # Generate dummy data for 6 months and 2 ODS codes
        start_date = datetime(2021, 1, 1)
        ods_codes = [
            {'code': 'RK9', 'name': 'University Hospitals Plymouth NHS Trust'},
            {'code': 'RT2', 'name': 'Pennine Care NHS Foundation Trust'}
        ]
        
        dummy_data = []
        for i in range(6):
            current_date = start_date + timedelta(days=30*i)
            for ods in ods_codes:
                dummy_data.append({
                    'year_month': current_date.strftime('%Y-%m-%d'),
                    'vmp_code': '42206511000001102',
                    'vmp_name': 'Apixaban 5mg tablets',
                    'ods_code': ods['code'],
                    'ods_name': ods['name'],
                    'SCMD_quantity': 1000 + (i * 100) + (500 if ods['code'] == 'RK9' else 0),
                    'SCMD_quantity_basis': 'tablet',
                    'dose_quantity': 1000 + (i * 100) + (500 if ods['code'] == 'RK9' else 0),
                    'converted_udfs': 1.0,
                    'udfs_basis': 'tablet',
                    'dose_unit': 'tablet',
                    'df_ind': 'Discrete'
                })
        
        context['dummy_data'] = dummy_data
    
        return context
