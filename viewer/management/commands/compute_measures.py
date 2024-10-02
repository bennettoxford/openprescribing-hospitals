from django.core.management.base import BaseCommand
from django.db import transaction
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
                values = result['values']['measure_values']
               
                org_data = defaultdict(lambda: defaultdict(float))

                for row in values:
                    month = datetime.strptime(row['month'], "%Y-%m").strftime("%Y-%m-%d")
                    org_data[row['organisation']][month] = row['quantity']

                org_ods_names = set(org_data.keys())
                organisations = {org.ods_name: org for org in Organisation.objects.filter(ods_name__in=org_ods_names)}

                precomputed_measures = []
                for org, months in org_data.items():
                    organisation = organisations.get(org)
                    if organisation:
                        for month, quantity in months.items():
                            precomputed_measures.append(
                                PrecomputedMeasure(
                                    measure=measure,
                                    organisation=organisation,
                                    month=month,
                                    quantity=quantity
                                )
                            )

                with transaction.atomic():
                    PrecomputedMeasure.objects.bulk_create(
                        precomputed_measures,
                        update_conflicts=True,
                        update_fields=['quantity'],
                        unique_fields=['measure', 'organisation', 'month'],
                        batch_size=1000
                    )

                self.stdout.write(self.style.SUCCESS(f"Successfully computed measure: {measure.name}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error computing measure {measure.name}: {e}"))