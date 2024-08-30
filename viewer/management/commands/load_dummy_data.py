import csv
from datetime import datetime
from django.core.management.base import BaseCommand
from django.conf import settings
from viewer.models import Dose, Organisation
import os

class Command(BaseCommand):
    help = 'Loads dummy data from CSV into the database'

    def handle(self, *args, **options):
        csv_path = os.path.join(settings.BASE_DIR, 'dummy_data.csv')
        org_csv_path = os.path.join(settings.BASE_DIR, 'organisations.csv')

        if not os.path.exists(csv_path) or not os.path.exists(org_csv_path):
            self.stdout.write(self.style.ERROR(f'CSV file(s) not found'))
            return

        Organisation.objects.all().delete()
        Dose.objects.all().delete()

        # Load organisations
        with open(org_csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            organisations = []
            for row in reader:
                org = Organisation(
                    ods_code=row['ods_code'],
                    ods_name=row['ods_name'],
                    region=row['region']
                )
                organisations.append(org)
            Organisation.objects.bulk_create(organisations)

        # Load doses
        with open(csv_path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            doses = []
            for row in reader:
                org = Organisation.objects.get(ods_code=row['ods_code'])
                dose = Dose(
                    year_month=datetime.strptime(row['year_month'], '%Y-%m-%d').date(),
                    vmp_code=row['vmp_code'],
                    vmp_name=row['vmp_name'],
                    organisation=org,
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

        self.stdout.write(self.style.SUCCESS(f'Successfully loaded {len(organisations)} organisations and {len(doses)} doses into the database'))