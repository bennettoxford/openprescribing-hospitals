import csv
from datetime import datetime
from django.core.management.base import BaseCommand
from django.conf import settings
import os
from viewer.models import VTM, VMP, Ingredient, Organisation, Dose, SCMD, IngredientQuantity

class Command(BaseCommand):
    help = 'Loads dummy data from CSV files into the database'

    def handle(self, *args, **options):
        dummy_data_dir = os.path.join(settings.BASE_DIR, 'dummy_data')

        self.load_vtm(dummy_data_dir)
        self.load_vmp(dummy_data_dir)
        self.load_ingredient(dummy_data_dir)
        self.load_organisation(dummy_data_dir)
        self.load_dose(dummy_data_dir)
        self.load_scmd(dummy_data_dir)
        self.load_ingredient_quantity(dummy_data_dir)

        self.stdout.write(self.style.SUCCESS('Successfully loaded all dummy data into the database'))

    def load_csv(self, filename, directory):
        filepath = os.path.join(directory, f'{filename}.csv')
        with open(filepath, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            return list(reader)

    def load_vtm(self, directory):
        vtms = self.load_csv('vtm', directory)
        VTM.objects.bulk_create([VTM(**vtm) for vtm in vtms])
        self.stdout.write(self.style.SUCCESS(f'Loaded {len(vtms)} VTMs'))

    def load_vmp(self, directory):
        vmps = self.load_csv('vmp', directory)
        VMP.objects.bulk_create([
            VMP(code=vmp['code'], name=vmp['name'], vtm_id=vmp['vtm'])
            for vmp in vmps
        ])
        self.stdout.write(self.style.SUCCESS(f'Loaded {len(vmps)} VMPs'))

    def load_ingredient(self, directory):
        ingredients = self.load_csv('ingredient', directory)
        Ingredient.objects.bulk_create([Ingredient(**ingredient) for ingredient in ingredients])
        self.stdout.write(self.style.SUCCESS(f'Loaded {len(ingredients)} Ingredients'))

    def load_organisation(self, directory):
        organisations = self.load_csv('organisation', directory)
        Organisation.objects.bulk_create([
            Organisation(ods_code=org['ods_code'], ods_name=org['ods_name'], region=org['region'])
            for org in organisations
        ])
        self.stdout.write(self.style.SUCCESS(f'Loaded {len(organisations)} Organisations'))

    def load_dose(self, directory):
        doses = self.load_csv('dose', directory)
        Dose.objects.bulk_create([
            Dose(
                year_month=datetime.strptime(dose['year_month'], '%Y-%m-%d').date(),
                vmp_id=dose['vmp'],
                quantity=float(dose['quantity']),
                unit=dose['unit'],
                organisation_id=dose['organisation']
            )
            for dose in doses
        ])
        self.stdout.write(self.style.SUCCESS(f'Loaded {len(doses)} Doses'))

    def load_scmd(self, directory):
        scmds = self.load_csv('scmd', directory)
        SCMD.objects.bulk_create([
            SCMD(
                year_month=datetime.strptime(scmd['year_month'], '%Y-%m-%d').date(),
                vmp_id=scmd['vmp'],
                quantity=float(scmd['quantity']),
                unit=scmd['unit'],
                organisation_id=scmd['organisation']
            )
            for scmd in scmds
        ])
        self.stdout.write(self.style.SUCCESS(f'Loaded {len(scmds)} SCMDs'))

    def load_ingredient_quantity(self, directory):
        ingredient_quantities = self.load_csv('ingredient_quantity', directory)
        IngredientQuantity.objects.bulk_create([
            IngredientQuantity(
                year_month=datetime.strptime(iq['year_month'], '%Y-%m-%d').date(),
                ingredient_id=iq['ingredient'],
                vmp_id=iq['vmp'],
                quantity=float(iq['quantity']),
                unit=iq['unit'],
                organisation_id=iq['organisation']
            )
            for iq in ingredient_quantities
        ])
        self.stdout.write(self.style.SUCCESS(f'Loaded {len(ingredient_quantities)} IngredientQuantities'))