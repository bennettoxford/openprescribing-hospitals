from django.core.management.base import BaseCommand
from viewer.models import Measure, PrecomputedMeasure, Organisation
from viewer.measures.measure_utils import execute_measure_sql
from collections import defaultdict
from datetime import datetime

class Command(BaseCommand):
    help = 'Compute and store measures in the database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--measure',
            type=str,
            help='Specify a measure to compute. If not provided, all measures will be computed.'
        )

    def handle(self, *args, **kwargs):
        measure_name = kwargs.get('measure')
        if measure_name:
            measures = Measure.objects.filter(name=measure_name)
            if not measures.exists():
                self.stdout.write(self.style.ERROR(f"Measure {measure_name} does not exist."))
                return
        else:
            measures = Measure.objects.all()

        for measure in measures:
            self.stdout.write(f"Computing measure: {measure.name}")
            try:
                result = execute_measure_sql(measure.name)
                values = result.get("values", [])

                org_data = defaultdict(lambda: defaultdict(float))
                for row in values:
                    month = row['month']
                    org = row['organisation']
                    value = row['quantity']
                    # Ensure the month is in YYYY-MM-DD format
                    month = datetime.strptime(month, "%Y-%m").strftime("%Y-%m-%d")
                    org_data[org][month] = value

                for org, months in org_data.items():
                    organisation = Organisation.objects.get(ods_name=org)
                    for month, quantity in months.items():
                        PrecomputedMeasure.objects.update_or_create(
                            measure=measure,
                            organisation=organisation,
                            month=month,
                            defaults={'quantity': quantity}
                        )
                self.stdout.write(self.style.SUCCESS(f"Successfully computed measure: {measure.name}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error computing measure {measure.name}: {e}"))