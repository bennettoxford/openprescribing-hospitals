import csv
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.conf import settings
import os
import random

class Command(BaseCommand):
    help = 'Generates CSV files with dummy data for all models'

    def handle(self, *args, **options):
        # Create a 'dummy_data' folder in the project's base directory
        dummy_data_dir = os.path.join(settings.BASE_DIR, 'dummy_data')
        os.makedirs(dummy_data_dir, exist_ok=True)

        start_date = datetime(2020, 11, 1)
        end_date = datetime(2021, 4, 1)

        vtms = [
            {'vtm': 'VTM001', 'name': 'Apixaban'},
            {'vtm': 'VTM002', 'name': 'Rivaroxaban'},
            {'vtm': 'VTM003', 'name': 'Dabigatran'},
        ]

        vmps = [
            {'code': '42206511000001102', 'name': 'Apixaban 5mg tablets', 'vtm': 'VTM001'},
            {'code': '14254711000001104', 'name': 'Rivaroxaban 10mg tablets', 'vtm': 'VTM002'},
            {'code': '38728311000001104', 'name': 'Dabigatran etexilate 150mg capsules', 'vtm': 'VTM003'},
        ]

        ingredients = [
            {'code': 'ING001', 'name': 'Apixaban'},
            {'code': 'ING002', 'name': 'Rivaroxaban'},
            {'code': 'ING003', 'name': 'Dabigatran etexilate'},
        ]

        organisations = [
            {'ods_code': 'RK9', 'ods_name': 'University Hospitals Plymouth NHS Trust', 'region': 'South West'},
            {'ods_code': 'RT2', 'ods_name': 'Pennine Care NHS Foundation Trust', 'region': 'North West'},
            {'ods_code': 'RXY', 'ods_name': 'Kent And Medway NHS And Social Care Partnership Trust', 'region': 'South East'},
        ]

        # Generate CSV files
        self.generate_vtm_csv(vtms, dummy_data_dir)
        self.generate_vmp_csv(vmps, dummy_data_dir)
        self.generate_ingredient_csv(ingredients, dummy_data_dir)
        self.generate_organisation_csv(organisations, dummy_data_dir)
        self.generate_dose_csv(start_date, end_date, vmps, organisations, dummy_data_dir)
        self.generate_scmd_csv(start_date, end_date, vmps, organisations, dummy_data_dir)
        self.generate_ingredient_quantity_csv(start_date, end_date, ingredients, vmps, organisations, dummy_data_dir)

        self.stdout.write(self.style.SUCCESS(f'Successfully generated all dummy data CSV files in {dummy_data_dir}'))

    def generate_csv(self, filename, fieldnames, data, directory):
        filepath = os.path.join(directory, f'{filename}.csv')
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        self.stdout.write(self.style.SUCCESS(f'Generated {filename}.csv in {directory}'))

    def generate_vtm_csv(self, vtms, directory):
        self.generate_csv('vtm', ['vtm', 'name'], vtms, directory)

    def generate_vmp_csv(self, vmps, directory):
        self.generate_csv('vmp', ['code', 'name', 'vtm'], vmps, directory)

    def generate_ingredient_csv(self, ingredients, directory):
        self.generate_csv('ingredient', ['code', 'name'], ingredients, directory)

    def generate_organisation_csv(self, organisations, directory):
        self.generate_csv('organisation', ['ods_code', 'ods_name', 'region'], organisations, directory)

    def generate_dose_csv(self, start_date, end_date, vmps, organisations, directory):
        doses = []
        current_date = start_date
        while current_date <= end_date:
            for vmp in vmps:
                for org in organisations:
                    doses.append({
                        'year_month': current_date.strftime('%Y-%m-%d'),
                        'vmp': vmp['code'],
                        'quantity': random.randint(100, 1000),
                        'unit': 'tablet',
                        'organisation': org['ods_code'],
                    })
            current_date += timedelta(days=30)
        self.generate_csv('dose', ['year_month', 'vmp', 'quantity', 'unit', 'organisation'], doses, directory)

    def generate_scmd_csv(self, start_date, end_date, vmps, organisations, directory):
        scmds = []
        current_date = start_date
        while current_date <= end_date:
            for vmp in vmps:
                for org in organisations:
                    scmds.append({
                        'year_month': current_date.strftime('%Y-%m-%d'),
                        'vmp': vmp['code'],
                        'quantity': random.randint(80, 120),
                        'unit': 'tablet',
                        'organisation': org['ods_code'],
                    })
            current_date += timedelta(days=30)
        self.generate_csv('scmd', ['year_month', 'vmp', 'quantity', 'unit', 'organisation'], scmds, directory)

    def generate_ingredient_quantity_csv(self, start_date, end_date, ingredients, vmps, organisations, directory):
        ingredient_quantities = []
        current_date = start_date
        while current_date <= end_date:
            for ingredient in ingredients:
                for vmp in vmps:
                    for org in organisations:
                        ingredient_quantities.append({
                            'year_month': current_date.strftime('%Y-%m-%d'),
                            'ingredient': ingredient['code'],
                            'vmp': vmp['code'],
                            'quantity': random.randint(50, 150),
                            'unit': 'mg',
                            'organisation': org['ods_code'],
                        })
            current_date += timedelta(days=30)
        self.generate_csv('ingredient_quantity', ['year_month', 'ingredient', 'vmp', 'quantity', 'unit', 'organisation'], ingredient_quantities, directory)