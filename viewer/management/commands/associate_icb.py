from django.core.management.base import BaseCommand
from django.conf import settings
import pandas as pd
from viewer.models import Organisation
from pathlib import Path
import os

class Command(BaseCommand):
    help = 'Associates ICBs with existing organisations based on CSV data'

    def handle(self, *args, **options):
        # Get the path to the CSV file using settings
        data_dir = os.path.join(settings.BASE_DIR, "data")
        csv_path = Path(data_dir, "organisation_table.csv")
        
        if not csv_path.exists():
            self.stdout.write(self.style.ERROR(f'File not found: {csv_path}'))
            return

        updated_count = 0
        skipped_count = 0

        # Read CSV using pandas
        df = pd.read_csv(csv_path)
        
        for _, row in df.iterrows():
            try:
                org = Organisation.objects.get(ods_code=row['ods_code'])
                org.icb = row['icb']
                org.save()
                updated_count += 1
            except Organisation.DoesNotExist:
                skipped_count += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"Organisation not found: {row['ods_code']}"
                    )
                )

        self.stdout.write(
            self.style.SUCCESS(
                f'Successfully updated {updated_count} organisations with ICB data'
            )
        )
        if skipped_count:
            self.stdout.write(
                self.style.WARNING(
                    f'Skipped {skipped_count} organisations (not found in database)'
                )
            )
