import math

from django.db.models import F
from tqdm import tqdm
from django.db.models import Case, When, IntegerField, CharField
from django.core.management.base import BaseCommand

from viewer.models import (
    Measure,
    MeasureVMP, 
    PrecomputedMeasure, 
    PrecomputedMeasureAggregated, 
    PrecomputedPercentile,
    Dose,
    IngredientQuantity,
    DDDQuantity,
    Organisation
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

        # Map quantity_type string to actual model
        model_mapping = {
            'dose': Dose,
            'ingredient': IngredientQuantity,
            'ddd': DDDQuantity
        }

        model = model_mapping.get(measure.quantity_type)
        if not model:
            self.stdout.write(
                self.style.ERROR(f'Invalid quantity_type: {measure.quantity_type}')
            )
            return

        org_successor_map = {}
        org_icb_map = {}
        org_region_map = {}

        for org in Organisation.objects.all():
            if org.successor:
                org_successor_map[org.id] = org.successor.id
            else:
                org_successor_map[org.id] = org.id

            org_icb_map[org.id] = org.icb
            org_region_map[org.id] = org.region

        

        subset = model.objects.filter(vmp__in=measure.vmps.all())

        # annotate with the icb and region
        subset = subset.annotate(icb=F('organisation__icb'), region=F('organisation__region'))
        
        # annotate with the successor id (if an org has no successor, set it to itself)
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
        measure_icbs = subset.values_list('normalised_icb_id', flat=True).distinct()
        measure_regions = subset.values_list('normalised_region_id', flat=True).distinct()  
        

        # get the measurevmps associated with this measure
        measurevmps = MeasureVMP.objects.filter(measure=measure)

        def get_consistent_unit(queryset):
            """
            Get the consistent unit from a queryset's data arrays.
            Raises ValueError if units are inconsistent.
            """
            first_unit = None
            for record in queryset:
                for entry in record.data or []:
                    if len(entry) >= 3 and entry[2]:  # if unit exists
                        if first_unit is None:
                            first_unit = entry[2]
                        elif entry[2] != first_unit:
                            raise ValueError(f"Inconsistent units found: {first_unit} vs {entry[2]}")
            return first_unit

        # Add this after getting the measurevmps
        for measurevmp in measurevmps:
            # Get all quantity records for this VMP
            vmp_records = subset.filter(vmp=measurevmp.vmp)
            
            try:
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

        def get_monthly_sums(queryset):
            monthly_sums = {}
            for record in queryset:
                for entry in record.data or []:
                    month = entry[0]
                    if entry[1]:  # if quantity exists
                        if month not in monthly_sums:
                            monthly_sums[month] = 0
                        monthly_sums[month] += float(entry[1])
            return monthly_sums

        # Calculate sums for numerator and denominator
        numerator_records = subset.filter(vmp__in=numerator_vmps)
        denominator_records = subset.filter(vmp__in=denominator_vmps)

        org_values = {}

        for org_id in measure_orgs:
            # Get monthly sums for numerator
            num_monthly = get_monthly_sums(
                numerator_records.filter(normalised_org_id=org_id)
            )
            
            # Get monthly sums for denominator
            den_monthly = get_monthly_sums(
                denominator_records.filter(normalised_org_id=org_id)
            )
            

            # Initialize org dictionary if not exists
            if org_id not in org_values:
                org_values[org_id] = {}
            
            # For each month that appears in either numerator or denominator
            all_months = set(num_monthly.keys()) | set(den_monthly.keys())
            for month in all_months:
                num_value = num_monthly.get(month, 0)
                den_value = den_monthly.get(month, 0)
                
                org_values[org_id][month] = {
                    "numerator": num_value,
                    "denominator": den_value,
                    "value": (num_value / den_value * 100) if den_value else 0
                }
        

        # populate precomputed measures
        precomputed_measures = []
        for org_id, values in tqdm(org_values.items()):
            for month, value in values.items():
                precomputed_measures.append(
                    PrecomputedMeasure(
                        measure=measure,
                        organisation=Organisation.objects.get(id=org_id),
                        month=month,
                        numerator=value["numerator"],
                        denominator=value["denominator"],
                        quantity=value["value"]
                    )
                )
        
        PrecomputedMeasure.objects.bulk_create(precomputed_measures)
        self.stdout.write(
            self.style.SUCCESS(f'Successfully created {len(precomputed_measures)} precomputed measures')
        )

        # Calculate percentiles
        precomputed_percentiles = []
        
        # Get all months across all data
        all_months = set()
        for org_data in org_values.values():
            all_months.update(org_data.keys())
        
        # Ensure every org has every month
        for org_id in measure_orgs:
            if org_id not in org_values:
                org_values[org_id] = {}
            
            # Add missing months with zero values
            for month in all_months:
                if month not in org_values[org_id]:
                    org_values[org_id][month] = {
                        "numerator": 0,
                        "denominator": 0,
                        "value": 0
                    }
        
        # For each month, calculate percentiles
        for month in all_months:
            # Get ALL values for this month (convert None to 0)
            month_values = [
                org_data[month]['value'] if org_data[month]['value'] is not None else 0
                for org_data in org_values.values()
            ]
            
            # Now all values will be numbers, so sort will work
            month_values.sort()
            n = len(month_values)
            
            # Calculate percentiles from 5 to 95 in steps of 10 (but also include 50)
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
        
        # Bulk create all percentile records
        PrecomputedPercentile.objects.bulk_create(precomputed_percentiles)
        self.stdout.write(
            self.style.SUCCESS(f'Successfully created {len(precomputed_percentiles)} percentile records')
        )


        icb_values = {}

        for icb_id in measure_icbs:
            # Get monthly sums for numerator
            num_monthly = get_monthly_sums(
                numerator_records.filter(normalised_icb_id=icb_id)
            )
            
            # Get monthly sums for denominator
            den_monthly = get_monthly_sums(
                denominator_records.filter(normalised_icb_id=icb_id)
            )
            

            # Initialize org dictionary if not exists
            if icb_id not in icb_values:
                icb_values[icb_id] = {}
            
            # For each month that appears in either numerator or denominator
            all_months = set(num_monthly.keys()) | set(den_monthly.keys())
            for month in all_months:
                num_value = num_monthly.get(month, 0)
                den_value = den_monthly.get(month, 0)
                
                icb_values[icb_id][month] = {
                    "numerator": num_value,
                    "denominator": den_value,
                    "value": (num_value / den_value * 100) if den_value else None
                }
        
        for label, values in icb_values.items():
            for month, value in values.items():
                PrecomputedMeasureAggregated.objects.create(
                    measure=measure,
                    label=label,
                    month=month,
                    numerator=value["numerator"],
                    denominator=value["denominator"],
                    quantity=value["value"],
                    category='icb'
                )

        region_values = {}

        for region_id in measure_regions:
            # Get monthly sums for numerator
            num_monthly = get_monthly_sums(
                numerator_records.filter(normalised_region_id=region_id)
            )
            
            # Get monthly sums for denominator
            den_monthly = get_monthly_sums(
                denominator_records.filter(normalised_region_id=region_id)
            )
            

            # Initialize org dictionary if not exists
            if region_id not in region_values:
                region_values[region_id] = {}
            
            # For each month that appears in either numerator or denominator
            all_months = set(num_monthly.keys()) | set(den_monthly.keys())
            for month in all_months:
                num_value = num_monthly.get(month, 0)
                den_value = den_monthly.get(month, 0)
                
                region_values[region_id][month] = {
                    "numerator": num_value,
                    "denominator": den_value,
                    "value": (num_value / den_value * 100) if den_value else None
                }
        
        for label, values in region_values.items():
            for month, value in values.items():
                PrecomputedMeasureAggregated.objects.create(
                    measure=measure,
                    label=label,
                    month=month,
                    numerator=value["numerator"],
                    denominator=value["denominator"],
                    quantity=value["value"],
                    category='region'
                )

        national_values = {}

        num_monthly = get_monthly_sums(
                numerator_records
            )
            
        # Get monthly sums for denominator
        den_monthly = get_monthly_sums(
            denominator_records
        )
        
        # For each month that appears in either numerator or denominator
        all_months = set(num_monthly.keys()) | set(den_monthly.keys())
        for month in all_months:
            num_value = num_monthly.get(month, 0)
            den_value = den_monthly.get(month, 0)
            
            national_values[month] = {
                "numerator": num_value,
                "denominator": den_value,
                "value": (num_value / den_value * 100) if den_value else None
            }

        for month, value in national_values.items():
            PrecomputedMeasureAggregated.objects.create(
                measure=measure,
                label='National',
                month=month,
                numerator=value["numerator"],
                denominator=value["denominator"],
                quantity=value["value"],
                category='national'
            )

