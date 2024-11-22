from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Sum
from viewer.models import OrgSubmissionCache, SCMDQuantity, Organisation
from tqdm import tqdm

def update_org_submission_cache():
    with transaction.atomic():
        OrgSubmissionCache.objects.all().delete()
     
        unique_months = list(SCMDQuantity.objects.dates('year_month', 'month').order_by('year_month'))
        organisations = list(Organisation.objects.select_related('successor').all())

        print("Fetching quantity data...")
        quantity_sums = {}
        for month in tqdm(unique_months, desc="Processing quantity sums"):
            quantity_sums[month] = {
                org.ods_code: SCMDQuantity.objects.filter(
                    organisation__ods_code__in=[org.ods_code] + org.get_all_predecessor_codes(),
                    year_month=month
                ).aggregate(Sum('quantity'))['quantity__sum'] or 0
                for org in organisations
            }

        print("Creating cache objects...")
        cache_objects = []
        
        for month in tqdm(unique_months, desc="Processing months"):
            for org in organisations:
              
                vmp_count = org.get_combined_vmp_count(month)
                has_submitted = vmp_count > 0
                
                quantity_sum = quantity_sums[month][org.ods_code]

                cache_objects.append(OrgSubmissionCache(
                    organisation=org,
                    successor=org.successor,
                    month=month,
                    has_submitted=has_submitted,
                    vmp_count=vmp_count,
                    quantity_count=quantity_sum
                ))


        if cache_objects:
            OrgSubmissionCache.objects.bulk_create(cache_objects)

    print("Successfully updated org submission cache")

class Command(BaseCommand):
    help = 'Updates the organisation submission cache'

    def handle(self, *args, **options):
        update_org_submission_cache()
        self.stdout.write(self.style.SUCCESS('Successfully updated org submission cache'))