import pandas as pd
from pathlib import Path
from django.core.management.base import BaseCommand
from viewer.models import VMP, OntFormRoute
from tqdm import tqdm
from django.db.models import F
from viewer.management.utils import PROJECT_ROOT
from collections import defaultdict

class Command(BaseCommand):
    help = "Gets the form for all VMPs and updates if there's a conflict."

    def handle(self, *args, **options):
        try:
            form_data = pd.read_csv(Path(PROJECT_ROOT, "data/vmp_form_table.csv"), dtype={"vmp_code": "string"})
            form_dict = dict(zip(form_data["vmp_code"], form_data["dform_form"]))

            ont_form_routes_data = pd.read_csv(Path(PROJECT_ROOT, "data/vmp_ontform_table.csv"), dtype={"vmp": "string"})
            ont_form_routes_dict = defaultdict(list)
            for _, row in ont_form_routes_data.iterrows():
                ont_form_routes_dict[row['vmp']].append(row['descr'])

            vmps_to_update = []
            ont_form_routes_to_add = defaultdict(list)

            for vmp in tqdm(VMP.objects.all()):
                updated = False
                if vmp.code in form_dict:
                    new_form = form_dict[vmp.code]
                    if vmp.form != new_form:
                        vmp.form = new_form
                        updated = True

                if vmp.code in ont_form_routes_dict:
                    ont_form_routes_to_add[vmp].extend(ont_form_routes_dict[vmp.code])

                if updated:
                    vmps_to_update.append(vmp)
                else:
                    self.stdout.write(self.style.WARNING(f"VMP {vmp.code} not found in form data"))

            VMP.objects.bulk_update(vmps_to_update, ['form'])

            all_routes = set(route for routes in ont_form_routes_to_add.values() for route in routes)
            OntFormRoute.objects.bulk_create([OntFormRoute(name=route) for route in all_routes], ignore_conflicts=True)

            for vmp, routes in ont_form_routes_to_add.items():
                new_routes = OntFormRoute.objects.filter(name__in=routes)
                vmp.ont_form_routes.set(new_routes)

            self.stdout.write(self.style.SUCCESS(
                f"Successfully updated forms for {len(vmps_to_update)} VMPs and added ontological form routes"))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"An error occurred: {str(e)}"))
