import numpy as np
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Avg, Min, Max, Sum
from viewer.models import Measure, PrecomputedMeasure, PrecomputedMeasureAggregated, Organisation, PrecomputedPercentile, VMP
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
                with transaction.atomic():
                    self.delete_existing_precomputed_data(measure)

                    result = execute_measure_sql(measure.name)
                    values = result['values']
                    measure_values = values['measure_values']
                    
                    # Filter out organisations that have no activity (where both numerator and denominator are 0 or null)
                    active_orgs = {
                        row['organisation'] for row in measure_values 
                        if (row['numerator'] or row['denominator']) and 
                        not (row['numerator'] == 0 and row['denominator'] == 0)
                    }
                    
                    measure_values = [
                        row for row in measure_values 
                        if row['organisation'] in active_orgs
                    ]

                    numerator_vmps = values.get('numerator_vmps', [])
                    denominator_vmps = values.get('denominator_vmps', [])
                    
                    vmp_dict = {vmp.code: vmp for vmp in VMP.objects.filter(code__in=set(numerator_vmps + denominator_vmps))}
                    
                    numerator_vmp_objects = [vmp_dict[code] for code in numerator_vmps if code in vmp_dict]
                    denominator_vmp_objects = [vmp_dict[code] for code in denominator_vmps if code in vmp_dict]
                    
                    measure.numerator_vmps.set(numerator_vmp_objects)
                    measure.denominator_vmps.set(denominator_vmp_objects)
                    
                    org_data = defaultdict(lambda: defaultdict(lambda: {'numerator': None, 'denominator': None}))

                    for row in measure_values:
                        month = datetime.strptime(row['month'], "%Y-%m").strftime("%Y-%m-%d")
                        org_data[row['organisation']][month]['numerator'] = row['numerator']
                        org_data[row['organisation']][month]['denominator'] = row['denominator']

                    org_ods_names = set(org_data.keys())
                    organisations = {org.ods_name: org for org in Organisation.objects.filter(ods_name__in=org_ods_names)}

                    precomputed_measures = []
                    for org, months in org_data.items():
                        organisation = organisations.get(org)
                        if organisation:
                            for month, data in months.items():
                                numerator = data['numerator']
                                denominator = data['denominator']

                                if denominator and denominator != 0:
                                    quantity = numerator / denominator
                                elif denominator and denominator == 0:
                                    quantity = 0
                                else:
                                    quantity = None
                                    
                                precomputed_measure = PrecomputedMeasure(
                                    measure=measure,
                                    organisation=organisation,
                                    month=month,
                                    numerator=numerator,
                                    denominator=denominator,
                                    quantity=quantity
                                )
                                precomputed_measures.append(precomputed_measure)

                    PrecomputedMeasure.objects.bulk_create(
                        precomputed_measures,
                        update_conflicts=True,
                        update_fields=['numerator', 'denominator', 'quantity'],
                        unique_fields=['measure', 'organisation', 'month'],
                        batch_size=100
                    )

                self.calculate_and_store_aggregations(measure)
                self.calculate_and_store_percentiles(measure)

                self.stdout.write(self.style.SUCCESS(f"Successfully computed measure: {measure.name}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error computing measure {measure.name}: {str(e)}"))
                self.stdout.write(self.style.ERROR(f"Result keys: {result.keys() if 'result' in locals() else 'Result not available'}"))

    def delete_existing_precomputed_data(self, measure):
        PrecomputedMeasure.objects.filter(measure=measure).delete()
        PrecomputedMeasureAggregated.objects.filter(measure=measure).delete()
        PrecomputedPercentile.objects.filter(measure=measure).delete()
        self.stdout.write(f"Deleted existing precomputed data for measure: {measure.name}")

    def calculate_and_store_aggregations(self, measure):
        region_aggregations = (
            PrecomputedMeasure.objects
            .filter(measure=measure)
            .values('organisation__region', 'month')
            .annotate(
                numerator_sum=Sum('numerator'),
                denominator_sum=Sum('denominator')
            )
        )

        icb_aggregations = (
            PrecomputedMeasure.objects
            .filter(measure=measure)
            .values('organisation__icb', 'month')
            .annotate(
                numerator_sum=Sum('numerator'),
                denominator_sum=Sum('denominator')
            )
        )

        national_aggregations = (
            PrecomputedMeasure.objects
            .filter(measure=measure)
            .values('month')
            .annotate(
                numerator_sum=Sum('numerator'),
                denominator_sum=Sum('denominator')
            )
        )

        aggregated_measures = []

        for agg in region_aggregations:
            quantity = agg['numerator_sum'] / agg['denominator_sum'] if agg['denominator_sum'] and agg['denominator_sum'] != 0 else None
            aggregated_measures.append(
                PrecomputedMeasureAggregated(
                    measure=measure,
                    label=agg['organisation__region'],
                    month=agg['month'],
                    numerator=agg['numerator_sum'],
                    denominator=agg['denominator_sum'],
                    quantity=quantity,
                    category='region'
                )
            )

        for agg in icb_aggregations:
            quantity = agg['numerator_sum'] / agg['denominator_sum'] if agg['denominator_sum'] and agg['denominator_sum'] != 0 else None
            aggregated_measures.append(
                PrecomputedMeasureAggregated(
                    measure=measure,
                    label=agg['organisation__icb'],
                    month=agg['month'],
                    numerator=agg['numerator_sum'],
                    denominator=agg['denominator_sum'],
                    quantity=quantity,
                    category='icb'
                )
            )

        for agg in national_aggregations:
            quantity = agg['numerator_sum'] / agg['denominator_sum'] if agg['denominator_sum'] and agg['denominator_sum'] != 0 else None
            aggregated_measures.append(
                PrecomputedMeasureAggregated(
                    measure=measure,
                    label='National',
                    month=agg['month'],
                    numerator=agg['numerator_sum'],
                    denominator=agg['denominator_sum'],
                    quantity=quantity,
                    category='national'
                )
            )

        with transaction.atomic():
            PrecomputedMeasureAggregated.objects.bulk_create(
                aggregated_measures,
                update_conflicts=True,
                update_fields=['numerator', 'denominator', 'quantity'],
                unique_fields=['measure', 'category', 'label', 'month'],
                batch_size=100
            )

    def calculate_and_store_percentiles(self, measure):
        date_range = PrecomputedMeasure.objects.filter(measure=measure).aggregate(
            min_date=Min('month'),
            max_date=Max('month')
        )
        
        min_date, max_date = date_range['min_date'], date_range['max_date']
        
        all_months = [min_date + relativedelta(months=i) for i in range((max_date.year - min_date.year) * 12 + max_date.month - min_date.month + 1)]

        percentile_values = [5, 15, 25, 35, 45, 50, 55, 65, 75, 85, 95]
        
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
                            quantity=0
                        )
                    )

        with transaction.atomic():
            PrecomputedPercentile.objects.bulk_create(
                percentiles_to_create,
                update_conflicts=True,
                update_fields=['quantity'],
                unique_fields=['measure', 'month', 'percentile'],
                batch_size=100
            )
