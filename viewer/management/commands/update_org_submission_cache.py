from django.core.management.base import BaseCommand
from django.db import connection
from viewer.models import OrgSubmissionCache, SCMDQuantity, Organisation, DataStatus
from tqdm import tqdm
from collections import defaultdict

def update_org_submission_cache():
    print("Clearing existing cache...")
    with connection.cursor() as cursor:
        cursor.execute(f'TRUNCATE TABLE {OrgSubmissionCache._meta.db_table} RESTART IDENTITY CASCADE')
    
    print("Fetching months and organizations...")
    unique_months = list(DataStatus.objects.dates('year_month', 'month').order_by('year_month'))
    month_strings = {m.strftime('%Y-%m-%d') for m in unique_months}
    
    organisations = Organisation.objects.select_related('successor').prefetch_related('predecessors').all()
    
    org_hierarchy = defaultdict(list)
    for org in organisations:
        org_codes = [org.ods_code] + list(org.predecessors.values_list('ods_code', flat=True))
        for org_code in org_codes:
            org_hierarchy[org_code].append(org.ods_code)

    scmd_queryset = SCMDQuantity.objects.values(
        'organisation__ods_code',
        'vmp_id', 
        'data'
    ).iterator(chunk_size=10000)
    
    org_month_vmps = defaultdict(lambda: defaultdict(set))
    
    print(f"Processing {(SCMDQuantity.objects.count())} SCMD records")
    for scmd_record in tqdm(scmd_queryset, desc="Processing SCMD data"):
        org_code = scmd_record['organisation__ods_code']
        vmp_id = scmd_record['vmp_id']
        data_array = scmd_record['data'] or []
        
        for data_entry in data_array:
            if len(data_entry) >= 2 and data_entry[0] and data_entry[1]:
                month_str = data_entry[0]
                
                if month_str not in month_strings:
                    continue
                    
                try:
                    quantity = float(data_entry[1])
                    if quantity > 0:
                        for ancestor_org in org_hierarchy[org_code]:
                            org_month_vmps[ancestor_org][month_str].add(vmp_id)
                        
                except (ValueError, TypeError):
                    continue

    print("Creating cache objects...")
    for org in tqdm(organisations, desc="Processing organizations"):
        cache_objects = []
        
        for month in unique_months:
            month_str = month.strftime('%Y-%m-%d')
            unique_vmps = org_month_vmps[org.ods_code].get(month_str, set())
            
            has_submitted = len(unique_vmps) > 0
            vmp_count = len(unique_vmps) if has_submitted else None
            quantity_count = len(unique_vmps) if has_submitted else None
            
            cache_objects.append(OrgSubmissionCache(
                organisation=org,
                successor=org.successor,
                month=month,
                has_submitted=has_submitted,
                vmp_count=vmp_count,
                quantity_count=quantity_count
            ))
        
        if cache_objects:
            OrgSubmissionCache.objects.bulk_create(cache_objects, batch_size=1000)

    print("Successfully updated org submission cache")

class Command(BaseCommand):
    help = 'Updates the organisation submission cache'

    def handle(self, *args, **options):
        update_org_submission_cache()
        self.stdout.write(self.style.SUCCESS('Successfully updated org submission cache'))