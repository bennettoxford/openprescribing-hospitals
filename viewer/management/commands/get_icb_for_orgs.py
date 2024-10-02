import requests

from django.core.management.base import BaseCommand
from viewer.models import Organisation
from tqdm import tqdm


BASE_URL = "https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations"


class Command(BaseCommand):
    help = """
    Gets the ICB for all organisations.

    Only the ICB code is currently in BigQuery
    """

    def handle(self, *args, **options):
        organisations = Organisation.objects.all()

        for organisation in tqdm(organisations):
            
            organisation = Organisation.objects.get(ods_code=organisation.ods_code)
            self.get_icb_by_ods_code(organisation)
           
        self.stdout.write(self.style.SUCCESS(
            "Successfully got the ICB for all organisations"))

    def get_icb_by_ods_code(self, organisation):
        response = requests.get(f"{BASE_URL}/{organisation.ods_code}")
        
        relationships = response.json()["Organisation"]["Rels"]["Rel"]
        for relationship in relationships:
            if relationship["id"] == "RE5" and relationship["Status"] == "Active":
                icb_code = relationship["Target"]["OrgId"]["extension"]
                icb_name = self.get_icb_name_by_code(icb_code)
                organisation.icb = icb_name
                organisation.save()
        

    def get_icb_name_by_code(self, icb_code):
        response = requests.get(f"{BASE_URL}/{icb_code}")
        icb_name = response.json()["Organisation"]["Name"]
        return icb_name
