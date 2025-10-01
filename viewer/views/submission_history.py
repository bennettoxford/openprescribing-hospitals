from django.views.generic import TemplateView
from django.db.models import Max
from ..mixins import MaintenanceModeMixin
from ..models import DataStatus, OrgSubmissionCache, Organisation
from ..utils import get_organisation_data
from datetime import date
import json
from django.utils.safestring import mark_safe
from datetime import datetime
from collections import defaultdict


class SubmissionHistoryView(MaintenanceModeMixin, TemplateView):
    template_name = 'submission_history.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        shared_org_data = get_organisation_data()
        
        all_orgs = Organisation.objects.select_related('successor', 'region', 'icb').values(
            'ods_code', 'ods_name', 'successor__ods_name', 'region__name', 'region__code', 'icb__name', 'icb__code'
        ).order_by('ods_name')
        
        org_data_template = defaultdict(lambda: {
            'successor': None, 
            'submissions': {}, 
            'predecessors': [], 
            'ods_code': None,
            'region': None,
            'region_code': None,
            'icb': None,
            'icb_code': None
        })
        
        regions_with_icbs = {}
        
        for org in all_orgs:
            name = org['ods_name']
            code = org['ods_code']
            successor_name = org['successor__ods_name']
            region = org['region__name']
            region_code = org['region__code']
            icb = org['icb__name']
            icb_code = org['icb__code']
            
            org_data_template[name]['ods_code'] = code
            org_data_template[name]['successor'] = successor_name
            org_data_template[name]['region'] = region
            org_data_template[name]['region_code'] = region_code
            org_data_template[name]['icb'] = icb
            org_data_template[name]['icb_code'] = icb_code
            
            if not successor_name:
                if region not in regions_with_icbs:
                    regions_with_icbs[region] = {'icbs': set(), 'region_code': region_code}
                if icb:
                    regions_with_icbs[region]['icbs'].add((icb, icb_code))
            
            if successor_name:
                org_data_template[successor_name]['predecessors'].append(name)
        
        hierarchy = [
            {
                'region': region,
                'region_code': data['region_code'],
                'icbs': sorted([{'name': icb_name, 'code': icb_code} for icb_name, icb_code in data['icbs']], key=lambda x: x['name'])
            }
            for region, data in sorted(regions_with_icbs.items())
        ]
        
        context['regions_hierarchy'] = json.dumps(hierarchy)
        
        # Get latest dates for each file type
        latest_dates = {}
        for file_type in ['final']:
            latest = DataStatus.objects.filter(
                file_type=file_type
            ).aggregate(
                latest_date=Max('year_month')
            )['latest_date']
            if latest:
                latest_dates[file_type] = latest.strftime("%B %Y")
            else:
                latest_dates[file_type] = None
        
        context['latest_dates'] = json.dumps(latest_dates)

        org_data = org_data_template
        all_dates = set()
        
        for cache in OrgSubmissionCache.objects.select_related('organisation', 'successor').order_by('organisation__ods_name', 'month'):
            org_name = cache.organisation.ods_name
            month_str = cache.month.isoformat() if isinstance(cache.month, date) else str(cache.month)
            org_data[org_name]['submissions'][month_str] = {
                'has_submitted': cache.has_submitted,
                'vmp_count': cache.vmp_count or 0
            }
            all_dates.add(month_str)

        restructured_data = []
        processed_orgs = set()

        def build_org_hierarchy(org_name):
            org_entry = {
                'name': org_name,
                'ods_code': org_data[org_name]['ods_code'],
                'data': org_data[org_name]['submissions'],
                'predecessors': []
            }
            processed_orgs.add(org_name)

            # Sort predecessors to ensure consistent ordering
            sorted_predecessors = sorted(org_data[org_name]['predecessors'])
            for pred in sorted_predecessors:
                if pred not in processed_orgs:
                    org_entry['predecessors'].append(build_org_hierarchy(pred))

            return org_entry

        # Process current organisations (those without successors) first
        current_orgs = sorted([org for org, data in org_data.items() if not data['successor']])
        for org in current_orgs:
            if org not in processed_orgs:
                restructured_data.append(build_org_hierarchy(org))

        # Process any remaining organisations
        for org in sorted(org_data.keys()):
            if org not in processed_orgs:
                restructured_data.append(build_org_hierarchy(org))

        latest_date = max(all_dates) if all_dates else None
        for org_entry in restructured_data:
            org_entry['latest_submission'] = org_entry['data'].get(latest_date, False)

        def assign_region_icb(org_entry):
            org_name = org_entry['name']
            if org_name in org_data:
                org_info = org_data[org_name]
                if org_info['successor']:
                    # Use successor's region/ICB if this org has a successor
                    successor_info = org_data.get(org_info['successor'], {})
                    org_entry['region'] = successor_info.get('region', org_info.get('region'))
                    org_entry['region_code'] = successor_info.get('region_code', org_info.get('region_code'))
                    org_entry['icb'] = successor_info.get('icb', org_info.get('icb'))
                    org_entry['icb_code'] = successor_info.get('icb_code', org_info.get('icb_code'))
                else:
                    # Use own region/ICB
                    org_entry['region'] = org_info.get('region')
                    org_entry['region_code'] = org_info.get('region_code')
                    org_entry['icb'] = org_info.get('icb')
                    org_entry['icb_code'] = org_info.get('icb_code')
            
            for pred in org_entry.get('predecessors', []):
                assign_region_icb(pred)

        for org_entry in restructured_data:
            assign_region_icb(org_entry)

        context['org_data_json'] = mark_safe(json.dumps({
            'organisations': restructured_data,
            'org_codes': shared_org_data['org_codes'],
            'predecessor_map': shared_org_data['predecessor_map']
        }))

        if all_dates:
            earliest_date = min(all_dates)
            latest_date = max(all_dates)
            
            context['earliest_date'] = datetime.strptime(earliest_date, "%Y-%m-%d").strftime("%B %Y")
            context['latest_date'] = datetime.strptime(latest_date, "%Y-%m-%d").strftime("%B %Y")
        else:
            context['earliest_date'] = None
            context['latest_date'] = None

        return context
