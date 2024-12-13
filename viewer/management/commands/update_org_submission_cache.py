from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Sum
from viewer.models import OrgSubmissionCache, SCMDQuantity, Organisation, DataStatus
from tqdm import tqdm

def update_org_submission_cache():
    with transaction.atomic():
        OrgSubmissionCache.objects.all().delete()
     
        unique_months = list(DataStatus.objects.dates('year_month', 'month').order_by('year_month'))
        organisations = list(Organisation.objects.select_related('successor').all())

        print("Fetching quantity data...")

        vmp_counts = {}
        quantity_sums = {}

        for org in tqdm(organisations, desc="Processing organisations"):
            quantity_sums[org.ods_code] = {}
            vmp_counts[org.ods_code] = {}
            org_list = [org.ods_code] + org.get_all_predecessor_codes()
            subset = SCMDQuantity.objects.filter(
                organisation__ods_code__in=org_list
            )
            
            for month in unique_months:
                month_str = month.strftime('%Y-%m-%d')
                
                    
                month_subset = subset.filter(data__contains=[[month_str]])

                if month_subset.exists():
                    month_subset_data = list(month_subset.values('data').first()['data'])
                    # this is an array of arrays, each containing a single array of 3 elements
                    # we want to sum the second element of each of these arrays
                    quantity_sum = sum(float(subarray[1]) for subarray in month_subset_data)

                    unique_vmp_count = month_subset.values('vmp').distinct().count()
                    
                    quantity_sums[org.ods_code][month_str] = quantity_sum
                    vmp_counts[org.ods_code][month_str] = unique_vmp_count
                else:
                    quantity_sums[org.ods_code][month_str] = None
                    vmp_counts[org.ods_code][month_str] = None

        cache_objects = []
        for org in tqdm(organisations, desc="Creating cache objects"):
            for month in unique_months:
                month_str = month.strftime('%Y-%m-%d')
                cache_objects.append(OrgSubmissionCache(
                    organisation=org,
                    successor=org.successor,
                    month=month,
                    has_submitted=vmp_counts[org.ods_code][month_str] is not None,
                    vmp_count=vmp_counts[org.ods_code][month_str],
                    quantity_count=quantity_sums[org.ods_code][month_str]
                ))
   
        if cache_objects:
            OrgSubmissionCache.objects.bulk_create(cache_objects)

    print("Successfully updated org submission cache")

class Command(BaseCommand):
    help = 'Updates the organisation submission cache'

    def handle(self, *args, **options):
        update_org_submission_cache()
        self.stdout.write(self.style.SUCCESS('Successfully updated org submission cache'))