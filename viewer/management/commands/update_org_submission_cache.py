from django.core.management.base import BaseCommand

from django.db import transaction

from dateutil.relativedelta import relativedelta
from viewer.models import OrgSubmissionCache, Dose, Organisation
from django.db.models import Min, Max, F, Exists, OuterRef
from django.db.models.functions import TruncMonth
from tqdm import tqdm

def update_org_submission_cache():
    with transaction.atomic():
        # Clear existing cache
        OrgSubmissionCache.objects.all().delete()

        # Get unique months and date range
        unique_months = Dose.objects.dates('year_month', 'month').order_by('year_month')
        min_date = unique_months.first()
        max_date = unique_months.last()

        # Get all organisations with their successors
        organisations = list(Organisation.objects.select_related('successor').all())

        # Prepare bulk create list
        cache_objects = []

        print("Creating cache objects...")
        for month in tqdm(unique_months, desc="Processing months"):
            # Get organisations with doses for this month
            orgs_with_doses = set(Dose.objects.filter(year_month__month=month.month, 
                                                      year_month__year=month.year)
                                   .values_list('organisation__ods_code', flat=True))

            for org in organisations:
                has_submitted = org.ods_code in orgs_with_doses
                cache_objects.append(OrgSubmissionCache(
                    organisation=org,
                    successor=org.successor,  # Add the successor information
                    month=month,
                    has_submitted=has_submitted
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