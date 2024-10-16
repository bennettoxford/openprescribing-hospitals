import numpy as np
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Avg, Min, Max
from viewer.models import Measure, PrecomputedMeasure, PrecomputedMeasureAggregated, Organisation, PrecomputedPercentile
from viewer.measures.measure_utils import execute_measure_sql
from collections import defaultdict
from datetime import datetime
from dateutil.relativedelta import relativedelta

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
            measures = Measure.objects.filter(short_name=measure_name)
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

                self.calculate_and_store_aggregations(measure)
                self.calculate_and_store_percentiles(measure)

                self.stdout.write(self.style.SUCCESS(f"Successfully computed measure: {measure.name}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error computing measure {measure.name}: {e}"))

    def calculate_and_store_aggregations(self, measure):

        region_aggregations = (
            PrecomputedMeasure.objects
            .filter(measure=measure)
            .values('organisation__region', 'month')
            .annotate(quantity=Avg('quantity'))
        )

        icb_aggregations = (
            PrecomputedMeasure.objects
            .filter(measure=measure)
            .values('organisation__icb', 'month')
            .annotate(quantity=Avg('quantity'))
        )

        aggregated_measures = []

        for agg in region_aggregations:
            aggregated_measures.append(
                PrecomputedMeasureAggregated(
                    measure=measure,
                    label=agg['organisation__region'],
                    month=agg['month'],
                    quantity=agg['quantity'],
                    category='region'
                )
            )

        for agg in icb_aggregations:
            aggregated_measures.append(
                PrecomputedMeasureAggregated(
                    measure=measure,
                    label=agg['organisation__icb'],
                    month=agg['month'],
                    quantity=agg['quantity'],
                    category='icb'
                )
            )

        with transaction.atomic():
            PrecomputedMeasureAggregated.objects.bulk_create(
                aggregated_measures,
                update_conflicts=True,
                update_fields=['quantity'],
                unique_fields=['measure', 'category', 'label', 'month'],
                batch_size=1000
            )

    def calculate_and_store_percentiles(self, measure):
        date_range = PrecomputedMeasure.objects.filter(measure=measure).aggregate(
            min_date=Min('month'),
            max_date=Max('month')
        )
        
        min_date, max_date = date_range['min_date'], date_range['max_date']
        
        all_months = [min_date + relativedelta(months=i) for i in range((max_date.year - min_date.year) * 12 + max_date.month - min_date.month + 1)]

        percentile_values = [5, 15, 25, 35, 45, 55, 65, 75, 85, 95]
        
        percentiles_to_create = []

        for month in all_months:
            values = PrecomputedMeasure.objects.filter(
                measure=measure,
                month=month,
                quantity__isnull=False
            ).values_list('quantity', flat=True)

            values = list(filter(None, values))
            
            if values:
                percentile_results = np.percentile(values, percentile_values)
                
                for percentile, value in zip(percentile_values, percentile_results):
                    percentiles_to_create.append(
                        PrecomputedPercentile(
                            measure=measure,
                            month=month,
                            percentile=percentile,
                            quantity=value
                        )
                    )
            else:
                for percentile in percentile_values:
                    percentiles_to_create.append(
                        PrecomputedPercentile(
                            measure=measure,
                            month=month,
                            percentile=percentile,
                            quantity=None
                        )
                    )

        with transaction.atomic():
            PrecomputedPercentile.objects.bulk_create(
                percentiles_to_create,
                update_conflicts=True,
                update_fields=['quantity'],
                unique_fields=['measure', 'month', 'percentile'],
                batch_size=1000
            )
