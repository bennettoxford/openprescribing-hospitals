import csv
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.conf import settings
import os

class Command(BaseCommand):
    help = 'Generates a CSV file with dummy data'

    def handle(self, *args, **options):
        start_date = datetime(2020, 11, 1)
        organisations = [
            {'code': 'RK9', 'name': 'University Hospitals Plymouth NHS Trust', 'region': 'South West'},
            {'code': 'RT2', 'name': 'Pennine Care NHS Foundation Trust', 'region': 'North West'},
            {'code': 'RXY', 'name': 'Kent And Medway NHS And Social Care Partnership Trust', 'region': 'South East'}
        ]
        medications = [
            {
                'vmp_code': '42206511000001102',
                'vmp_name': 'Apixaban 5mg tablets',
                'base_quantity': 1000
            },
            {
                'vmp_code': '14254711000001104',
                'vmp_name': 'Rivaroxaban 10mg tablets',
                'base_quantity': 38
            }
        ]

        csv_path = os.path.join(settings.BASE_DIR, 'dummy_data.csv')
        org_csv_path = os.path.join(settings.BASE_DIR, 'organisations.csv')

        # Generate organisations CSV
        with open(org_csv_path, 'w', newline='') as csvfile:
            fieldnames = ['ods_code', 'ods_name', 'region']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for org in organisations:
                writer.writerow({
                    'ods_code': org['code'],
                    'ods_name': org['name'],
                    'region': org['region']
                })

        # Generate doses CSV
        with open(csv_path, 'w', newline='') as csvfile:
            fieldnames = ['year_month', 'vmp_code', 'vmp_name', 'ods_code', 'SCMD_quantity',
                          'SCMD_quantity_basis', 'dose_quantity', 'converted_udfs', 'udfs_basis', 'dose_unit', 'df_ind']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for i in range(6):
                current_date = start_date + timedelta(days=30*i)
                for org in organisations:
                    for med in medications:
                        quantity = med['base_quantity'] + (i * 10) + (50 if org['code'] == 'RK9' else 0)
                        writer.writerow({
                            'year_month': current_date.strftime('%Y-%m-%d'),
                            'vmp_code': med['vmp_code'],
                            'vmp_name': med['vmp_name'],
                            'ods_code': org['code'],
                            'SCMD_quantity': quantity,
                            'SCMD_quantity_basis': 'tablet',
                            'dose_quantity': quantity,
                            'converted_udfs': 1.0,
                            'udfs_basis': 'tablet',
                            'dose_unit': 'tablet',
                            'df_ind': 'Discrete'
                        })

        self.stdout.write(self.style.SUCCESS(f'Successfully generated dummy data CSV at {csv_path}'))
        self.stdout.write(self.style.SUCCESS(f'Successfully generated organisations CSV at {org_csv_path}'))