from django.core.management.base import BaseCommand
from viewer.models import Organisation
from django.db import transaction

class Command(BaseCommand):
    help = "Fixes organisation names with incorrect apostrophe capitalisation"

    def handle(self, *args, **options):
        with transaction.atomic():
            orgs_updated = 0
            for org in Organisation.objects.all():
                original_name = org.ods_name
                fixed_name = original_name.replace("'S ", "'s ").replace("'S,", "'s,")
                
                if original_name != fixed_name:
                    org.ods_name = fixed_name
                    org.save()
                    orgs_updated += 1
                    self.stdout.write(f"Updated: {original_name} -> {fixed_name}")

            self.stdout.write(
                self.style.SUCCESS(f"Successfully updated {orgs_updated} organisation names")
            )