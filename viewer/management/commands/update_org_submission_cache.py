from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count, Sum
from viewer.models import OrgSubmissionCache, SCMDQuantity, Organisation
from tqdm import tqdm

def update_org_submission_cache():
    with transaction.atomic():
        # Clear existing cache
        OrgSubmissionCache.objects.all().delete()

        # Get unique months and date range
        unique_months = SCMDQuantity.objects.dates('year_month', 'month').order_by('year_month')
        
        # Get all organisations with their successors
        organisations = list(Organisation.objects.select_related('successor').all())

        # Prepare bulk create list
        cache_objects = []

        print("Creating cache objects...")
        for month in tqdm(unique_months, desc="Processing months"):
            # Get monthly stats for each organisation
            org_stats = SCMDQuantity.objects.filter(
                year_month__month=month.month,
                year_month__year=month.year
            ).values('organisation').annotate(
                vmp_count=Count('vmp', distinct=True),
                quantity_sum=Sum('quantity')
            )
            
            org_stats_dict = {
                stat['organisation']: {
                    'vmp_count': stat['vmp_count'],
                    'quantity_sum': stat['quantity_sum']
                } 
                for stat in org_stats
            }

            for org in organisations:
                stats = org_stats_dict.get(org.ods_code, {})
                has_submitted = stats.get('vmp_count', 0) > 0
                
                cache_objects.append(OrgSubmissionCache(
                    organisation=org,
                    successor=org.successor,
                    month=month,
                    has_submitted=has_submitted,
                    vmp_count=stats.get('vmp_count', 0),
                    quantity_count=stats.get('quantity_sum', 0)
                ))

        # Bulk create all cache objects
        print("Bulk creating cache objects...")
        OrgSubmissionCache.objects.bulk_create(cache_objects, batch_size=1000)

    print("Successfully updated org submission cache")

class Command(BaseCommand):
    help = 'Updates the organisation submission cache'

    def handle(self, *args, **options):
        update_org_submission_cache()
        self.stdout.write(self.style.SUCCESS('Successfully updated org submission cache'))