import csv
from datetime import datetime
from django.core.management.base import BaseCommand
from django.conf import settings
from viewer.models import Dose
import os

class Command(BaseCommand):
    help = 'Loads dummy data from CSV into the database'

    def handle(self, *args, **options):
        csv_path = os.path.join(settings.BASE_DIR, 'dummy_data.csv')

        if not os.path.exists(csv_path):
            self.stdout.write(self.style.ERROR(f'CSV file not found at {csv_path}'))
            return

        Dose.objects.all().delete()  # Clear existing data

        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            doses = []
            for row in reader:
                dose = Dose(
                    year_month=datetime.strptime(row['year_month'], '%Y-%m-%d').date(),
                    vmp_code=row['vmp_code'],
                    vmp_name=row['vmp_name'],
                    ods_code=row['ods_code'],
                    ods_name=row['ods_name'],
                    SCMD_quantity=float(row['SCMD_quantity']),
                    SCMD_quantity_basis=row['SCMD_quantity_basis'],
                    dose_quantity=float(row['dose_quantity']),
                    converted_udfs=float(row['converted_udfs']),
                    udfs_basis=row['udfs_basis'],
                    dose_unit=row['dose_unit'],
                    df_ind=row['df_ind']
                )
                doses.append(dose)

            Dose.objects.bulk_create(doses)

        self.stdout.write(self.style.SUCCESS(f'Successfully loaded {len(doses)} records into the database'))