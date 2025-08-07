import math
import time
from django.db.models import F
from django.db.models import Case, When, IntegerField, CharField
from django.core.management.base import BaseCommand
from django.db import transaction
from collections import defaultdict

from viewer.models import (
    Measure,
    MeasureVMP, 
    PrecomputedMeasure, 
    PrecomputedMeasureAggregated, 
    PrecomputedPercentile,
    Dose,
    IngredientQuantity,
    DDDQuantity,
    Organisation,
    IndicativeCost,
    SCMDQuantity
)


class Command(BaseCommand):
    help = 'Populates MeasureVMP instances for a given measure based on SQL file'

    def add_arguments(self, parser):
        parser.add_argument('measure', type=str, help='slug of the measure')

    def handle(self, *args, **kwargs):
        measure_slug = kwargs.get('measure')
        
        try:
            measure = Measure.objects.get(slug=measure_slug)
        except Measure.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f'Measure with slug "{measure_slug}" does not exist')
            )
            return

        def delete_existing_precomputed_data(measure):
            PrecomputedMeasure.objects.filter(measure=measure).delete()
            PrecomputedMeasureAggregated.objects.filter(measure=measure).delete()
            PrecomputedPercentile.objects.filter(measure=measure).delete()
            self.stdout.write(f"Deleted existing precomputed data for measure: {measure.slug}")

        delete_existing_precomputed_data(measure)

        model_mapping = {
            'scmd': SCMDQuantity,
            'dose': Dose,
            'ingredient': IngredientQuantity,
            'ddd': DDDQuantity,
            'indicative_cost': IndicativeCost,
        }

        model = model_mapping.get(measure.quantity_type)
        if not model:
            self.stdout.write(
                self.style.ERROR(f'Invalid quantity_type: {measure.quantity_type}')
            )
            return

        org_lookup = {org.id: org for org in Organisation.objects.all()}

        subset = model.objects.filter(vmp__in=measure.vmps.all())
        subset = subset.annotate(icb=F('organisation__icb'), region=F('organisation__region'))
        
        subset = subset.annotate(
            normalised_org_id=Case(
                When(organisation__successor_id__isnull=True, then=F('organisation_id')),
                default=F('organisation__successor_id'),
                output_field=IntegerField(),
            ),
            # normalised icb is the icb of the successor if it has one, otherwise the icb of the org
            normalised_icb_id=Case(
                When(organisation__successor__icb__isnull=True, then=F('organisation__icb')),
                default=F('organisation__successor__icb'),
                output_field=CharField(),
            ),
            # normalised region is the region of the successor if it has one, otherwise the region of the org
            normalised_region_id=Case(
                When(organisation__successor__region__isnull=True, then=F('organisation__region')),
                default=F('organisation__successor__region'),
                output_field=CharField(),
            )
        )

        measure_orgs = subset.values_list('normalised_org_id', flat=True).distinct()

        measurevmps = MeasureVMP.objects.filter(measure=measure)

        def get_consistent_unit(queryset):
            """
            Get the consistent unit from a queryset's data arrays.
            Raises ValueError if units are inconsistent.
            """
            first_unit = None
            for record in queryset:
                for entry in record.data or []:
                    if len(entry) >= 3 and entry[2]:
                        if first_unit is None:
                            first_unit = entry[2]
                        elif entry[2] != first_unit:
                            raise ValueError(f"Inconsistent units found: {first_unit} vs {entry[2]}")
            return first_unit

        for measurevmp in measurevmps:

            vmp_records = subset.filter(vmp=measurevmp.vmp)
            
            try:
                if measure.quantity_type == 'indicative_cost':
                    unit = '£'
                else:
                    unit = get_consistent_unit(vmp_records)
                    if unit:
                        measurevmp.unit = unit
                        measurevmp.save()
                    else:
                        self.stdout.write(
                            self.style.WARNING(f'No unit found for VMP: {measurevmp.vmp.name}')
                        )
            except ValueError as e:
                self.stdout.write(
                    self.style.ERROR(f'Error setting unit for VMP {measurevmp.vmp.name}: {str(e)}')
                )

        numerator_vmps = measurevmps.filter(type='numerator').values_list('vmp', flat=True)
        denominator_vmps = measurevmps.values_list('vmp', flat=True)

        numerator_records = subset.filter(vmp__in=numerator_vmps)
        denominator_records = subset.filter(vmp__in=denominator_vmps)

        numerator_data = (
            numerator_records
            .values('normalised_org_id', 'data')
            .iterator(chunk_size=1000)
        )
        
        denominator_data = (
            denominator_records
            .values('normalised_org_id', 'data')
            .iterator(chunk_size=1000)
        )

        org_monthly_data = defaultdict(lambda: defaultdict(lambda: {'numerator': 0, 'denominator': 0}))

        for record in numerator_data:
            org_id = record['normalised_org_id']
            for entry in record['data'] or []:
                if len(entry) >= 2 and entry[1]:
                    month = entry[0]
                    org_monthly_data[org_id][month]['numerator'] += float(entry[1])
        
        for record in denominator_data:
            org_id = record['normalised_org_id']
            for entry in record['data'] or []:
                if len(entry) >= 2 and entry[1]:
                    month = entry[0]
                    org_monthly_data[org_id][month]['denominator'] += float(entry[1])

        precomputed_measures = []
        for org_id, monthly_data in org_monthly_data.items():
            org = org_lookup[org_id]
            for month, values in monthly_data.items():
                numerator = values['numerator']
                denominator = values['denominator']
                quantity = (numerator / denominator * 100) if denominator else 0
                
                precomputed_measures.append(
                    PrecomputedMeasure(
                        measure=measure,
                        organisation=org,
                        month=month,
                        numerator=numerator,
                        denominator=denominator,
                        quantity=quantity
                    )
                )


        batch_size = 1000
        with transaction.atomic():
            for i in range(0, len(precomputed_measures), batch_size):
                batch = precomputed_measures[i:i + batch_size]
                PrecomputedMeasure.objects.bulk_create(batch, batch_size=batch_size)

        self.stdout.write(
            self.style.SUCCESS(f'Successfully created {len(precomputed_measures)} precomputed measures')
        )

        precomputed_percentiles = []
        
        all_months = set()
        for org_data in org_monthly_data.values():
            all_months.update(org_data.keys())
        
        for org_id in measure_orgs:
            if org_id not in org_monthly_data:
                org_monthly_data[org_id] = {}
            
            # Add missing months with zero values
            for month in all_months:
                if month not in org_monthly_data[org_id]:
                    org_monthly_data[org_id][month] = {
                        "numerator": 0,
                        "denominator": 0,
                        "value": 0
                    }
                else:
                    data = org_monthly_data[org_id][month]
                    data["value"] = (data["numerator"] / data["denominator"] * 100) if data["denominator"] else 0
        
        for month in all_months:
            month_values = [
                org_data[month]['value'] if org_data[month]['value'] is not None else 0
                for org_data in org_monthly_data.values()
            ]
            
            month_values.sort()
            n = len(month_values)
            
            for percentile in [5, 15, 25, 35, 45, 50, 55, 65, 75, 85, 95]:
                # Calculate index for this percentile
                k = (n - 1) * (percentile / 100)
                f = math.floor(k)
                c = math.ceil(k)
                
                if f == c:
                    value = month_values[int(k)]
                else:
                    # Interpolate when k is not an integer
                    d0 = month_values[int(f)] * (c - k)
                    d1 = month_values[int(c)] * (k - f)
                    value = d0 + d1
                
                precomputed_percentiles.append(
                    PrecomputedPercentile(
                        measure=measure,
                        month=month,
                        percentile=percentile,
                        quantity=value
                    )
                )
        
        PrecomputedPercentile.objects.bulk_create(precomputed_percentiles)
        self.stdout.write(
            self.style.SUCCESS(f'Successfully created {len(precomputed_percentiles)} percentile records')
        )


        self.stdout.write("Starting aggregations using existing org data...")
        start_time = time.time()

        icb_values = defaultdict(lambda: defaultdict(lambda: {'numerator': 0, 'denominator': 0}))
        region_values = defaultdict(lambda: defaultdict(lambda: {'numerator': 0, 'denominator': 0}))
        national_values = defaultdict(lambda: {'numerator': 0, 'denominator': 0})

        for org_id, monthly_data in org_monthly_data.items():
            org = org_lookup[org_id]
            
            icb_id = org.icb
            region_id = org.region
            
            for month, values in monthly_data.items():
                if icb_id:
                    icb_values[icb_id][month]['numerator'] += values['numerator']
                    icb_values[icb_id][month]['denominator'] += values['denominator']
                
                if region_id:
                    region_values[region_id][month]['numerator'] += values['numerator']
                    region_values[region_id][month]['denominator'] += values['denominator']
                
                national_values[month]['numerator'] += values['numerator']
                national_values[month]['denominator'] += values['denominator']
        
        for icb_id, monthly_data in icb_values.items():
            for month, values in monthly_data.items():
                values['value'] = (values['numerator'] / values['denominator'] * 100) if values['denominator'] else None

        for region_id, monthly_data in region_values.items():
            for month, values in monthly_data.items():
                values['value'] = (values['numerator'] / values['denominator'] * 100) if values['denominator'] else None

        for month, values in national_values.items():
            values['value'] = (values['numerator'] / values['denominator'] * 100) if values['denominator'] else None
        
        aggregation_time = time.time() - start_time
        self.stdout.write(f"All aggregations completed in: {aggregation_time:.2f}s (no additional queries!)")
        
        self.stdout.write(f"Found {len(icb_values)} ICBs, {len(region_values)} regions")


        def bulk_create_aggregated_measures(data_dict, category):
            aggregated_measures = []
            for label, monthly_data in data_dict.items():
                for month, values in monthly_data.items():
                    aggregated_measures.append(
                        PrecomputedMeasureAggregated(
                            measure=measure,
                            label=label,
                            month=month,
                            numerator=values["numerator"],
                            denominator=values["denominator"],
                            quantity=values["value"],
                            category=category
                        )
                    )
            
            with transaction.atomic():
                for i in range(0, len(aggregated_measures), batch_size):
                    batch = aggregated_measures[i:i + batch_size]
                    PrecomputedMeasureAggregated.objects.bulk_create(batch, batch_size=batch_size)
            
            return len(aggregated_measures)

        icb_count = bulk_create_aggregated_measures(icb_values, 'icb')
        region_count = bulk_create_aggregated_measures(region_values, 'region')
        national_count = bulk_create_aggregated_measures({'National': national_values}, 'national')

        self.stdout.write(
            self.style.SUCCESS(f'Successfully created {icb_count} ICB, {region_count} region, and {national_count} national aggregated measures')
        )

