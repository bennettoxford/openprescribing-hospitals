import csv
from datetime import datetime
from django.core.management.base import BaseCommand
from django.conf import settings
import os
import pandas as pd
from django.db import transaction
from viewer.models import (
    VTM,
    VMP,
    Ingredient,
    Organisation,
    Dose,
    IngredientQuantity,
    DataStatus,
    SCMDQuantity,
    Route,
    OntFormRoute,
    ATC,
    DDD
)
import glob
from tqdm import tqdm
import itertools
from django.db.models import Count


class Command(BaseCommand):
    help = "Loads @data into the database"

    def handle(self, *args, **options):
        data_dir = os.path.join(settings.BASE_DIR, "data")

        # self.load_vtm(data_dir)
        # self.load_ingredient(data_dir)
        # self.load_vmp(data_dir)
        # self.load_organisation(data_dir)
        # self.load_scmd_quantity(data_dir)
        # self.load_dose(data_dir)
        # self.load_ingredient_quantity(data_dir)
        # self.load_data_status(data_dir)
        # self.load_route(data_dir)
        # self.load_ont_form_route(data_dir)
        # self.load_atc(data_dir)
        # self.load_vmp_atc(data_dir)
        self.load_ddd(data_dir)

        self.stdout.write(self.style.SUCCESS(
            "Successfully loaded all data into the database"))

    def load_csv(self, filename, directory):
        filepath = os.path.join(directory, f"{filename}.csv")
        df = pd.read_csv(filepath)
        return df

    def load_vtm(self, directory):
        vtms = self.load_csv("vtm_table", directory)
        # drop any rows where vtm is null
        vtms = vtms[vtms["vtm"].notnull()]

        # drop duplicates
        vtms = vtms.drop_duplicates(subset=["vtm"])
        # convert vtm to int then to string
        vtms["vtm"] = vtms["vtm"].astype(int).astype(str)
        vtms.rename(columns={"vtm_name": "name"}, inplace=True)

        # to dict
        vtms = vtms.to_dict(orient="records")
        #
        VTM.objects.bulk_create(
            [VTM(vtm=vtm["vtm"], name=vtm["name"]) for vtm in vtms])
        self.stdout.write(self.style.SUCCESS(f"Loaded {len(vtms)} VTMs"))

    def load_ingredient(self, directory):
        ingredients = self.load_csv("ingredient_table", directory)
        # drop any rows where ingredient_code is null
        ingredients = ingredients[ingredients["ingredient_code"].notnull()]
        # print duplicates
        ingredients["ingredient_code"] = (
            ingredients["ingredient_code"].astype(int).astype(str)
        )
        # ingredients["ingredient_code"] = ingredients["ingredient_code"].apply(lambda x: str(int(x)) if pd.notnull(x) and x.is_integer() else '')
        ingredients.rename(
            columns={"ingredient_code": "code", "ing_nm": "name"}, inplace=True
        )

        # rename to match model
        ingredients = ingredients.to_dict(orient="records")
        Ingredient.objects.bulk_create(
            [
                Ingredient(code=ingredient["code"], name=ingredient["name"])
                for ingredient in ingredients
            ]
        )
        self.stdout.write(self.style.SUCCESS(
            f"Loaded {len(ingredients)} Ingredients"))

    def load_vmp(self, directory):
        vmps = self.load_csv("vmp_table", directory)

        # Drop any rows where vmp_code is null
        vmps = vmps[vmps["vmp_code"].notnull()]

        vmps.rename(
            columns={
                "vmp_code": "code",
                "vmp_name": "name"},
            inplace=True)
        vmps["code"] = vmps["code"].astype(int).astype(str)

        # there is a row for every vmp-ingredient pair, so we need to drop duplicates
        vmps_dropped_duplicates = vmps.drop_duplicates(subset=["code"])


        vmps_dropped_duplicates = vmps_dropped_duplicates.to_dict(
            orient="records")
        # Create VMPs

        VMP.objects.bulk_create(
            [VMP(code=vmp["code"], name=vmp["name"]) for vmp in vmps_dropped_duplicates]
        )

        vmp_code_to_id = dict(VMP.objects.values_list('code', 'id'))
        ingredient_code_to_id = dict(Ingredient.objects.values_list('code', 'id'))

        # Prepare ingredient and VTM relationships
        ingredient_relations = []
        vtm_updates = []

        for _, row in vmps.iterrows():
            if pd.notnull(row["ingredient"]):
                ing = str(int(row["ingredient"]))
                if row["code"] in vmp_code_to_id and ing in ingredient_code_to_id:
                    ingredient_relations.append(
                        VMP.ingredients.through(
                            vmp_id=vmp_code_to_id[row["code"]],
                            ingredient_id=ingredient_code_to_id[ing])
                    )

            if pd.notnull(row["vtm"]):
                vtm = str(int(row["vtm"]))
                vtm_updates.append({"code": row["code"], "vtm": vtm})

        # Bulk create ingredient relationships
        with transaction.atomic():
            VMP.ingredients.through.objects.bulk_create(
                ingredient_relations, ignore_conflicts=True
            )

        # Bulk update VTMs
        vtm_map = dict(VTM.objects.values_list('vtm', 'id'))
        vmp_objects = []
        for update in vtm_updates:
            if update["vtm"] in vtm_map:
                vmp = VMP.objects.get(code=update["code"])
                vmp.vtm_id = vtm_map[update["vtm"]]
                vmp_objects.append(vmp)

        VMP.objects.bulk_update(vmp_objects, ["vtm"], batch_size=1000)

        self.stdout.write(self.style.SUCCESS(
            f"Loaded {len(vmp_objects)} VMPs"))

    def load_organisation(self, directory):
        organisations = self.load_csv("organisation_table", directory)

        # Fix apostrophe capitalisation in organisation names. E.g. King'S -> King's
        organisations['ods_name'] = organisations['ods_name'].apply(
            lambda x: x.replace("'S ", "'s ").replace("'S,", "'s,") if isinstance(x, str) else x
        )

        # First pass: Create all organisations without setting successors
        org_objects = [
            Organisation(
                ods_code=org["ods_code"], ods_name=org["ods_name"], region=org["region"], icb=org["icb"]
            )
            for org in organisations.to_dict(orient="records")
        ]

        with transaction.atomic():
            Organisation.objects.bulk_create(
                org_objects, ignore_conflicts=True)

        # Second pass: Update successor relationships
        for org in organisations.to_dict(orient="records"):
            if org["successor_ods_code"] and not pd.isna(org["successor_ods_code"]):
                # Check if both organisations exist
                try:
                    current_org = Organisation.objects.get(ods_code=org["ods_code"])
                    successor_org = Organisation.objects.get(ods_code=org["successor_ods_code"])
                    current_org.successor = successor_org
                    current_org.save()
                except Organisation.DoesNotExist:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Organisation {org['ods_code']} or its successor {org['successor_ods_code']} not found. Skipping."
                        )
                    )

        self.stdout.write(
            self.style.SUCCESS(
                f"Loaded {len(org_objects)} Organisations and updated successor relationships"
            )
        )

    def calculate_atc_level(self, code):
        """
        Calculate the ATC level based on the length of the code
        """
        if len(code) == 1:
            return 1
        elif len(code) == 3:
            return 2
        elif len(code) == 4:
            return 3
        elif len(code) == 5:
            return 4
        elif len(code) == 7:
            return 5
        else:
            return 0

    def load_route(self, directory):
        routes = self.load_csv("route_table", directory)
        routes.rename(columns={"route_cd": "code", "route_descr": "name"}, inplace=True)
        routes["code"] = routes["code"].astype(str)

        unique_routes = routes.drop_duplicates(subset=["code", "name"])
        unique_routes = unique_routes.to_dict(orient="records")
        
        route_objects = [Route(code=route["code"], name=route["name"]) for route in unique_routes]
        Route.objects.bulk_create(route_objects, ignore_conflicts=True)

        # Get existing VMP codes and Route codes with their IDs
        vmp_code_to_id = dict(VMP.objects.values_list('code', 'id'))
        route_code_to_id = dict(Route.objects.values_list('code', 'id'))

        # Add routes to VMPs
        vmp_route_relations = []
        for _, row in routes.iterrows():
            if pd.notnull(row["vmp_code"]):
                vmp_code = str(int(row["vmp_code"]))
                if vmp_code in vmp_code_to_id and row["code"] in route_code_to_id:
                    vmp_route_relations.append(
                        VMP.routes.through(
                            vmp_id=vmp_code_to_id[vmp_code],
                            route_id=route_code_to_id[row["code"]]
                        )
                    )

        VMP.routes.through.objects.bulk_create(vmp_route_relations, ignore_conflicts=True)

        self.stdout.write(self.style.SUCCESS(f"Loaded {len(route_objects)} Routes and {len(vmp_route_relations)} VMP-Route relationships"))

    def load_monthly_csv(self, directory, pattern):
        files = glob.glob(os.path.join(directory, pattern))
        all_data = []
        for file in sorted(files):
            df = pd.read_csv(file)
            all_data.append(df)
        return pd.concat(all_data, ignore_index=True)

    def load_dose(self, directory):
        doses = self.load_monthly_csv(
            os.path.join(directory, "dose_quantity"),
            "*.csv")
        doses["vmp_code"] = doses["vmp_code"].astype(int).astype(str)

        # Get mappings using IDs
        organisations = dict(Organisation.objects.values_list('ods_code', 'id'))
        vmps = dict(VMP.objects.values_list('code', 'id'))

        # Filter valid doses and group by vmp and organisation
        valid_doses = doses[doses["ods_code"].isin(organisations) & doses["vmp_code"].isin(vmps)]
        grouped = valid_doses.groupby(['vmp_code', 'ods_code'])

        batch_size = 1000
        total_doses = 0
        dose_objects = []

        with tqdm(total=len(grouped), desc="Processing Dose") as pbar:
            for (vmp_code, ods_code), group in grouped:
                # Create array of [year_month, quantity, unit] for each group
                data_array = []
                for _, row in group.iterrows():
                    if pd.notnull(row.dose_quantity) and pd.notnull(row.dose_unit):
                        data_array.append([
                            row.year_month,
                            str(float(row.dose_quantity)),
                            row.dose_unit
                        ])
                
                if data_array:  # Only create object if there's data
                    dose_objects.append(
                        Dose(
                            vmp_id=vmps[vmp_code],
                            organisation_id=organisations[ods_code],
                            data=data_array
                        )
                    )

                if len(dose_objects) >= batch_size:
                    with transaction.atomic():
                        try:
                            Dose.objects.bulk_create(dose_objects, ignore_conflicts=True)
                            total_doses += len(dose_objects)
                        except Exception as e:
                            self.stdout.write(self.style.ERROR(f"Error creating batch: {str(e)}"))
                    dose_objects = []
                
                pbar.update(1)

            # Create any remaining objects
            if dose_objects:
                with transaction.atomic():
                    try:
                        Dose.objects.bulk_create(dose_objects, ignore_conflicts=True)
                        total_doses += len(dose_objects)
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Error creating batch: {str(e)}"))

        self.stdout.write(self.style.SUCCESS(f"Loaded {total_doses} Doses"))

    def batch_iterator(self, iterable, batch_size):
        iterator = iter(iterable)
        while True:
            batch = list(itertools.islice(iterator, batch_size))
            if not batch:
                break
            yield batch

    def load_ingredient_quantity(self, directory):
        ingredient_quantities = self.load_monthly_csv(
            os.path.join(directory, "ingredient_quantity"), "*.csv"
        )
        ingredient_quantities["vmp_code"] = ingredient_quantities["vmp_code"].astype(int).astype(str)
        ingredient_quantities["ingredient_code"] = ingredient_quantities["ingredient_code"].apply(
            lambda x: str(int(x)) if pd.notnull(x) else x
        )

        # Get mappings using IDs
        organisations = dict(Organisation.objects.values_list('ods_code', 'id'))
        vmps = dict(VMP.objects.values_list('code', 'id'))
        ingredients = dict(Ingredient.objects.values_list('code', 'id'))

        valid_iq = ingredient_quantities[
            ingredient_quantities["ods_code"].isin(organisations)
            & ingredient_quantities["vmp_code"].isin(vmps)
            & ingredient_quantities["ingredient_code"].isin(ingredients)
        ]

        batch_size = 1000
        total_iq = 0
        iq_objects = []

        # Group by ingredient, vmp, and organisation
        grouped = valid_iq.groupby(['ingredient_code', 'vmp_code', 'ods_code'])

        with tqdm(total=len(grouped), desc="Processing IngredientQuantities") as pbar:
            for (ingredient_code, vmp_code, ods_code), group in grouped:
                # Create array of [year_month, quantity, unit] for each group
                data_array = []
                for _, row in group.iterrows():
                    if pd.notnull(row.ingredient_quantity) and pd.notnull(row.ingredient_unit):
                        data_array.append([
                            row.year_month,
                            str(float(row.ingredient_quantity)),
                            row.ingredient_unit
                        ])
                
                if data_array:  # Only create object if there's data
                    iq_objects.append(
                        IngredientQuantity(
                            ingredient_id=ingredients[ingredient_code],
                            vmp_id=vmps[vmp_code],
                            organisation_id=organisations[ods_code],
                            data=data_array
                        )
                    )

                if len(iq_objects) >= batch_size:
                    with transaction.atomic():
                        try:
                            IngredientQuantity.objects.bulk_create(iq_objects, ignore_conflicts=True)
                            total_iq += len(iq_objects)
                        except Exception as e:
                            self.stdout.write(self.style.ERROR(f"Error creating batch: {str(e)}"))
                    iq_objects = []
                
                pbar.update(1)

            # Create any remaining objects
            if iq_objects:
                with transaction.atomic():
                    try:
                        IngredientQuantity.objects.bulk_create(iq_objects, ignore_conflicts=True)
                        total_iq += len(iq_objects)
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Error creating batch: {str(e)}"))

        self.stdout.write(self.style.SUCCESS(f"Loaded {total_iq} IngredientQuantities"))

    def load_data_status(self, directory):
        data_status = self.load_csv("data_status_table", directory)
        data_status["year_month"] = pd.to_datetime(data_status["year_month"])
        data_status_objects = [DataStatus(year_month=row["year_month"], file_type=row["file_type"]) for _, row in data_status.iterrows()]
        DataStatus.objects.bulk_create(data_status_objects)
        self.stdout.write(self.style.SUCCESS(
            f"Loaded {len(data_status_objects)} DataStatus"))
        
    def load_scmd_quantity(self, directory):
        doses = self.load_monthly_csv(
            os.path.join(directory, "dose_quantity"),
            "*.csv")
        doses["vmp_code"] = doses["vmp_code"].astype(int).astype(str)

        # Get mappings using IDs
        organisations = dict(Organisation.objects.values_list('ods_code', 'id'))
        vmps = dict(VMP.objects.values_list('code', 'id'))

        # Filter valid doses and group by vmp and organisation
        valid_doses = doses[doses["ods_code"].isin(organisations) & doses["vmp_code"].isin(vmps)]
        grouped = valid_doses.groupby(['vmp_code', 'ods_code'])

        batch_size = 1000
        total_scmd_quantities = 0
        scmd_objects = []

        with tqdm(total=len(grouped), desc="Processing SCMDQuantity") as pbar:
            for (vmp_code, ods_code), group in grouped:
                # Create array of [year_month, quantity, unit] for each group
                data_array = []
                for _, row in group.iterrows():
                    if pd.notnull(row.SCMD_quantity):
                        data_array.append([
                            row.year_month,
                            str(float(row.SCMD_quantity)),
                            row.SCMD_quantity_basis
                        ])
                
                if data_array:  # Only create object if there's data
                    scmd_objects.append(
                        SCMDQuantity(
                            vmp_id=vmps[vmp_code],
                            organisation_id=organisations[ods_code],
                            data=data_array
                        )
                    )
           
                if len(scmd_objects) >= batch_size:
                    with transaction.atomic():
                        try:
                            SCMDQuantity.objects.bulk_create(scmd_objects, ignore_conflicts=False)
                            total_scmd_quantities += len(scmd_objects)
                        except Exception as e:
                            self.stdout.write(self.style.ERROR(f"Error creating batch: {str(e)}"))
                    scmd_objects = []
                
                pbar.update(1)

            # Create any remaining objects
            if scmd_objects:
                with transaction.atomic():
                    try:
                        SCMDQuantity.objects.bulk_create(scmd_objects, ignore_conflicts=False)
                        total_scmd_quantities += len(scmd_objects)
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"Error creating batch: {str(e)}"))

        self.stdout.write(self.style.SUCCESS(f"Loaded {total_scmd_quantities} SCMDQuantities"))

    def load_ont_form_route(self, directory):
        ont_form_routes = self.load_csv("vmp_ontform_table", directory)
        ont_form_routes["vmp"] = ont_form_routes["vmp"].astype(str)
        
        unique_routes = ont_form_routes["descr"].unique()
        route_objects = [OntFormRoute(name=name) for name in unique_routes]
        OntFormRoute.objects.bulk_create(route_objects, ignore_conflicts=True)

        ont_form_route_ids = dict(OntFormRoute.objects.values_list('name', 'id'))
        vmp_ids = dict(VMP.objects.values_list('code', 'id'))

        vmp_route_relations = []
        for _, row in ont_form_routes.iterrows():
            if pd.notnull(row["vmp"]) and pd.notnull(row["descr"]):
                if row["vmp"] in vmp_ids and row["descr"] in ont_form_route_ids:
                    vmp_route_relations.append(
                        VMP.ont_form_routes.through(
                            vmp_id=vmp_ids[row["vmp"]],
                            ontformroute_id=ont_form_route_ids[row["descr"]]
                        )
                    )

        VMP.ont_form_routes.through.objects.bulk_create(vmp_route_relations, ignore_conflicts=True)

        self.stdout.write(self.style.SUCCESS(
            f"Loaded {len(route_objects)} OntFormRoutes and {len(vmp_route_relations)} VMP-OntFormRoute relationships"))

    def load_atc(self, directory):
        atcs = self.load_csv("atc_table", directory)
        
        # Clean the data
        atcs = atcs[atcs["atc_code"].notnull()]
        atcs = atcs[atcs["name"].notnull()]
        
        # Remove any trailing/leading whitespace
        atcs["atc_code"] = atcs["atc_code"].str.strip()
        atcs["name"] = atcs["name"].str.strip()
        
        # Sort by code length to ensure parents are created before children
        atcs = atcs.sort_values(by="atc_code", key=lambda x: x.str.len())
        
        # Create a dictionary to store code to ID mapping
        code_to_id = {}
        atc_objects = []
        # Process each ATC code
        for _, row in atcs.iterrows():
            code = row["atc_code"]
            name = row["name"]
            
            # Create ATC object
            atc = ATC(
                code=code,
                name=name,
            )
            atc_objects.append(atc)
        
    
        ATC.objects.bulk_create(atc_objects, ignore_conflicts=False)
        self.stdout.write(
            self.style.SUCCESS(
                f"Successfully loaded {len(atc_objects)} ATC codes"
            )
        )

    def load_vmp_atc(self, directory):
        vmp_atc = self.load_csv("atc_mapping_table", directory)
        
        # Convert VMP codes to strings
        vmp_atc["vmp_code"] = vmp_atc["vmp_code"].astype(str)
        vmp_atc["atc_code"] = vmp_atc["atc_code"].astype(str)
        
        # Get mappings for existing VMPs and ATCs
        vmp_ids = dict(VMP.objects.values_list('code', 'id'))
        atc_ids = dict(ATC.objects.values_list('code', 'id'))
        
        # Create VMP-ATC relationships
        vmp_atc_relations = []
        
        for _, row in vmp_atc.iterrows():
            if row["vmp_code"] in vmp_ids and row["atc_code"] in atc_ids:
                vmp_atc_relations.append(
                    VMP.atcs.through(
                        vmp_id=vmp_ids[row["vmp_code"]],
                        atc_id=atc_ids[row["atc_code"]]
                    )
                )
        
        # Bulk create the relationships
        with transaction.atomic():
            VMP.atcs.through.objects.bulk_create(
                vmp_atc_relations, 
                ignore_conflicts=True
            )
        
        self.stdout.write(
            self.style.SUCCESS(
                f"Successfully associated {len(vmp_atc_relations)} VMP-ATC relationships"
            )
        )
    
    def load_ddd(self, directory):
        ddd = self.load_csv("ddd_table", directory)
        ddd["atc_code"] = ddd["atc_code"].astype(str)
        ddd["ddd"] = ddd["ddd"].astype(float)
        ddd["unit_type"] = ddd["unit_type"].astype(str)
        ddd["dmd_route"] = ddd["dmd_route"].astype(str)

        # check if the VMP has a single route - each ddd is associated with a single route
        # so if a VMP has multiple routes, we don't know which ddd to use
        
        vmps_with_single_route = (
            VMP.objects
            .annotate(route_count=Count('routes'))
            .filter(route_count=1)
            .prefetch_related('atcs', 'routes')
        )

        vmp_route_dict = {
            (vmp.id, vmp.routes.all()[0].name): {
                'vmp': vmp,
                'route': vmp.routes.all()[0],
                'atc_codes': {atc.code for atc in vmp.atcs.all()}
            }
            for vmp in vmps_with_single_route
        }

        ddd_objects = []
        for _, row in tqdm(ddd.iterrows(), desc="Processing DDDs"):
            atc_code = row["atc_code"]
            dmd_route = row["dmd_route"]

            for (vmp_id, route_name), vmp_data in vmp_route_dict.items():
                if route_name == dmd_route and atc_code in vmp_data['atc_codes']:
                    ddd_objects.append(
                        DDD(
                            vmp=vmp_data['vmp'],
                            ddd=row["ddd"],
                            unit_type=row["unit_type"],
                            route=vmp_data['route']
                        )
                    )

        batch_size = 1000
        for i in range(0, len(ddd_objects), batch_size):
            batch = ddd_objects[i:i + batch_size]
            DDD.objects.bulk_create(batch, ignore_conflicts=True)

        self.stdout.write(self.style.SUCCESS(f"Loaded {len(ddd_objects)} DDDs"))
