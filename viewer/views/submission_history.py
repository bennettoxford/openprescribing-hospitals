from django.views.generic import TemplateView
from django.db.models import Max
from ..models import Organisation, DataStatus, OrgSubmissionCache
from datetime import date
import json
from django.utils.safestring import mark_safe
from collections import defaultdict
from datetime import datetime

class SubmissionHistoryView(TemplateView):
    template_name = 'submission_history.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        
        regions_with_icbs = {}
        for org in Organisation.objects.filter(successor__isnull=True).order_by('region', 'icb'):
            if org.region not in regions_with_icbs:
                regions_with_icbs[org.region] = set()
            if org.icb:
                regions_with_icbs[org.region].add(org.icb)
        
        hierarchy = [
            {
                'region': region,
                'icbs': sorted(list(icbs))
            }
            for region, icbs in sorted(regions_with_icbs.items())
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

        # Step 1: Collect data
        org_data = defaultdict(lambda: {'successor': None, 'submissions': {}, 'predecessors': []})
        all_dates = set()
        
        for cache in OrgSubmissionCache.objects.select_related('organisation', 'successor').order_by('organisation__ods_name', 'month'):
            org_name = cache.organisation.ods_name
            org_data[org_name]['successor'] = cache.successor.ods_name if cache.successor else None
            month_str = cache.month.isoformat() if isinstance(cache.month, date) else str(cache.month)
            org_data[org_name]['submissions'][month_str] = {
                'has_submitted': cache.has_submitted,
                'vmp_count': cache.vmp_count or 0
            }
            all_dates.add(month_str)

        # Step 2: Build predecessor relationships
        for org_name, data in org_data.items():
            if data['successor']:
                org_data[data['successor']]['predecessors'].append(org_name)

        # Step 3: Restructure data
        restructured_data = []
        processed_orgs = set()

        def build_org_hierarchy(org_name):
            org_entry = {
                'name': org_name,
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

        for org_entry in restructured_data:
            org = Organisation.objects.filter(ods_name=org_entry['name']).first()
            if org.successor:
                org_entry['region'] = org.successor.region
                org_entry['icb'] = org.successor.icb
            else:
                org_entry['region'] = org.region
                org_entry['icb'] = org.icb
            
            for pred in org_entry['predecessors']:
                pred_org = Organisation.objects.filter(ods_name=pred['name']).first()
                if pred_org.successor:
                    pred['region'] = pred_org.successor.region
                    pred['icb'] = pred_org.successor.icb
                else:
                    pred['region'] = pred_org.region
                    pred['icb'] = pred_org.icb

        context['org_data_json'] = mark_safe(json.dumps(restructured_data))

        if all_dates:
            earliest_date = min(all_dates)
            latest_date = max(all_dates)
            
            context['earliest_date'] = datetime.strptime(earliest_date, "%Y-%m-%d").strftime("%B %Y")
            context['latest_date'] = datetime.strptime(latest_date, "%Y-%m-%d").strftime("%B %Y")
        else:
            context['earliest_date'] = None
            context['latest_date'] = None

        return context
