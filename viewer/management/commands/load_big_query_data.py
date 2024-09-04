import csv
from datetime import datetime
from django.core.management.base import BaseCommand
from django.conf import settings
import os
import pandas as pd
from django.db import transaction
from viewer.models import VTM, VMP, Ingredient, Organisation, Dose, SCMD, IngredientQuantity
import glob
from tqdm import tqdm
import itertools

class Command(BaseCommand):
    help = 'Loads @data into the database'

    def handle(self, *args, **options):
        data_dir = os.path.join(settings.BASE_DIR, 'data')

        self.load_vtm(data_dir)
        self.load_ingredient(data_dir)
        self.load_vmp(data_dir)
        self.load_organisation(data_dir)
        self.load_scmd_and_dose(data_dir)
        self.load_ingredient_quantity(data_dir)

        self.stdout.write(self.style.SUCCESS('Successfully loaded all data into the database'))

    def load_csv(self, filename, directory):
        filepath = os.path.join(directory, f'{filename}.csv')
        df = pd.read_csv(filepath)
        return df

    def load_vtm(self, directory):
        vtms = self.load_csv('vtm_table', directory)
        # drop any rows where vtm is null
        vtms = vtms[vtms['vtm'].notnull()]

        # drop duplicates
        vtms = vtms.drop_duplicates(subset=["vtm"])
        # convert vtm to int then to string
        vtms["vtm"] = vtms["vtm"].astype(int).astype(str)
        vtms.rename(columns={'vtm_name': 'name'}, inplace=True)
        
        # to dict
        vtms = vtms.to_dict(orient='records')
        #
        VTM.objects.bulk_create([VTM(vtm=vtm['vtm'], name=vtm['name']) for vtm in vtms])
        self.stdout.write(self.style.SUCCESS(f'Loaded {len(vtms)} VTMs'))

    def load_ingredient(self, directory):
        ingredients = self.load_csv('ingredient_table', directory)
        # drop any rows where ingredient_code is null
        ingredients = ingredients[ingredients['ingredient_code'].notnull()]
        # print duplicates
        ingredients["ingredient_code"] = ingredients["ingredient_code"].astype(int).astype(str)
        # ingredients["ingredient_code"] = ingredients["ingredient_code"].apply(lambda x: str(int(x)) if pd.notnull(x) and x.is_integer() else '')
        ingredients.rename(columns={'ingredient_code': 'code', 'ing_nm': 'name'}, inplace=True)

        # rename to match model
        ingredients = ingredients.to_dict(orient='records')
        Ingredient.objects.bulk_create([
            Ingredient(code=ingredient['code'], name=ingredient['name'])
            for ingredient in ingredients
        ])
        self.stdout.write(self.style.SUCCESS(f'Loaded {len(ingredients)} Ingredients'))

    def load_vmp(self, directory):
        vmps = self.load_csv('vmp_table', directory)

        # Drop any rows where vmp_code is null
        vmps = vmps[vmps['vmp_code'].notnull()]

    
        vmps.rename(columns={'vmp_code': 'code', 'vmp_name': 'name'}, inplace=True)
        vmps["code"] = vmps["code"].astype(int).astype(str)


        vmps_dropped_duplicates = vmps.drop_duplicates(subset=["code"])

        vmps_dropped_duplicates = vmps_dropped_duplicates.to_dict(orient='records')
        # Create VMPs
        


        VMP.objects.bulk_create([VMP(code=vmp['code'], name=vmp['name']) for vmp in vmps_dropped_duplicates])

        # Prepare ingredient and VTM relationships
        ingredient_relations = []
        vtm_updates = []

        for _, row in vmps.iterrows():
            if pd.notnull(row['ingredient']):
                ing = str(int(row['ingredient']))
                ingredient_relations.append(VMP.ingredients.through(
                    vmp_id=row['code'],
                    ingredient_id=ing
                ))
            
            if pd.notnull(row['vtm']):
                vtm = str(int(row['vtm']))
                vtm_updates.append({'code': row['code'], 'vtm': vtm})

        # Bulk create ingredient relationships
        with transaction.atomic():
            VMP.ingredients.through.objects.bulk_create(ingredient_relations, ignore_conflicts=True)

        # Bulk update VTMs
        vtm_map = {vtm.vtm: vtm.vtm for vtm in VTM.objects.all()}
        vmp_objects = []
        for update in vtm_updates:
            if update['vtm'] in vtm_map:
                vmp = VMP(code=update['code'])
                vmp.vtm_id = vtm_map[update['vtm']]
                vmp_objects.append(vmp)

        VMP.objects.bulk_update(vmp_objects, ['vtm'], batch_size=1000)

        self.stdout.write(self.style.SUCCESS(f'Loaded {len(vmp_objects)} VMPs'))
 

    def load_organisation(self, directory):
        organisations = self.load_csv('organisation_table', directory)
        org_objects = [
            Organisation(
                ods_code=org['ods_code'],
                ods_name=org['ods_name'],
                region=org['region']
            )
            for org in organisations.to_dict(orient='records')
        ]
        
        with transaction.atomic():
            Organisation.objects.bulk_create(org_objects, ignore_conflicts=True)
        
        successor_updates = []
        for org in organisations.to_dict(orient='records'):
            if org['successor_ods_code'] and org['successor_ods_code'] != 'None':
                successor_updates.append(
                    Organisation(
                        ods_code=org['ods_code'],
                        successor_id=org['successor_ods_code']
                    )
                )
        
        with transaction.atomic():
            Organisation.objects.bulk_update(successor_updates, ['successor'], batch_size=100)
        
        self.stdout.write(self.style.SUCCESS(f'Loaded {len(org_objects)} Organisations and updated {len(successor_updates)} successor relationships'))

    def load_monthly_csv(self, directory, pattern):
        files = glob.glob(os.path.join(directory, pattern))
        all_data = []
        for file in sorted(files):
            df = pd.read_csv(file)
            all_data.append(df)
        return pd.concat(all_data, ignore_index=True)
    
    def load_scmd_and_dose(self, directory):
        doses = self.load_monthly_csv(os.path.join(directory, 'dose_quantity'), '*.csv')
        doses["vmp_code"] = doses["vmp_code"].astype(int).astype(str)
        
        organizations = {org.ods_code: org for org in Organisation.objects.all()}
        vmps = {vmp.code: vmp for vmp in VMP.objects.all()}
        
        # Filter out rows with missing org or vmp
        valid_doses = doses[doses['ods_code'].isin(organizations) & doses['vmp_code'].isin(vmps)]
        
        batch_size = 5000 
        total_doses = 0
        total_scmds = 0

 
        with tqdm(total=len(valid_doses), desc="Processing Doses and SCMDs") as pbar:
            for batch in self.batch_iterator(valid_doses.itertuples(index=False), batch_size):
                dose_objects = []
                scmd_objects = []
                
                for row in batch:
                    year_month = datetime.strptime(row.year_month, '%Y-%m-%d').date()
                    vmp = vmps[row.vmp_code]
                    org = organizations[row.ods_code]
                    
                    dose_objects.append(Dose(
                        year_month=year_month,
                        vmp=vmp,
                        quantity=float(row.dose_quantity) if pd.notnull(row.dose_quantity) else None,
                        unit=row.dose_unit,
                        organisation=org
                    ))
                    
                    scmd_objects.append(SCMD(
                        year_month=year_month,
                        vmp=vmp,
                        quantity=float(row.SCMD_quantity) if pd.notnull(row.SCMD_quantity) else None,
                        unit=row.SCMD_quantity_basis,
                        organisation=org
                    ))
                
                with transaction.atomic():
                    try:
                        Dose.objects.bulk_create(dose_objects)
                        SCMD.objects.bulk_create(scmd_objects)
                        total_doses += len(dose_objects)
                        total_scmds += len(scmd_objects)
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Error creating batch: {str(e)}"))
                
                pbar.update(len(batch))

        self.stdout.write(self.style.SUCCESS(f'Loaded {total_doses} Doses and {total_scmds} SCMDs'))

    def batch_iterator(self, iterable, batch_size):
        iterator = iter(iterable)
        while True:
            batch = list(itertools.islice(iterator, batch_size))
            if not batch:
                break
            yield batch

    def load_ingredient_quantity(self, directory):
        ingredient_quantities = self.load_monthly_csv(os.path.join(directory, 'ingredient_quantity'), '*.csv')
        ingredient_quantities["vmp_code"] = ingredient_quantities["vmp_code"].astype(int).astype(str)
        ingredient_quantities["ingredient_code"] = ingredient_quantities["ingredient_code"].apply(
            lambda x: str(int(x)) if pd.notnull(x) else x
        )

        organizations = {org.ods_code: org for org in Organisation.objects.all()}
        vmps = {vmp.code: vmp for vmp in VMP.objects.all()}
        ingredients = {ing.code: ing for ing in Ingredient.objects.all()}

        # Filter out rows with missing org, vmp, or ingredient
        valid_iq = ingredient_quantities[
            ingredient_quantities['ods_code'].isin(organizations) &
            ingredient_quantities['vmp_code'].isin(vmps) &
            ingredient_quantities['ingredient_code'].isin(ingredients)
        ]

        batch_size = 5000
        total_iq = 0


        with tqdm(total=len(valid_iq), desc="Processing IngredientQuantities") as pbar:
            for batch in self.batch_iterator(valid_iq.itertuples(index=False), batch_size):
                iq_objects = []
                
                for row in batch:
                    iq_objects.append(IngredientQuantity(
                        year_month=datetime.strptime(row.year_month, '%Y-%m-%d').date(),
                        ingredient=ingredients[row.ingredient_code],
                        vmp=vmps[row.vmp_code],
                        quantity=float(row.ingredient_quantity) if row.ingredient_quantity and str(row.ingredient_quantity).strip() else None,
                        unit=row.ingredient_unit,
                        organisation=organizations[row.ods_code]
                    ))
                
                with transaction.atomic():
                    try:
                        IngredientQuantity.objects.bulk_create(iq_objects)
                        total_iq += len(iq_objects)
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Error creating batch: {str(e)}"))
                
                pbar.update(len(batch))

        self.stdout.write(self.style.SUCCESS(f'Loaded {total_iq} IngredientQuantities'))