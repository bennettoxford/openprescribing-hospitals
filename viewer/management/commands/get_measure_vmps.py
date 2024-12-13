from django.core.management.base import BaseCommand
from django.db import transaction
from viewer.models import Measure, VMP, MeasureVMP, Dose, IngredientQuantity
from viewer.measures.measure_utils import execute_measure_sql

class Command(BaseCommand):
    help = 'Populates MeasureVMP instances for a given measure based on SQL file'

    def add_arguments(self, parser):
        parser.add_argument('measure_name', type=str, help='Short name of the measure')

    def handle(self, *args, **kwargs):
        measure_name = kwargs.get('measure_name')
        
        try:
            measure = Measure.objects.get(short_name=measure_name)
        except Measure.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f'Measure with short name "{measure_name}" does not exist')
            )
            return

        # Execute the measure's SQL file
        result = execute_measure_sql(measure.name)
        
        with transaction.atomic():
            # Clear existing MeasureVMP entries for this measure
            MeasureVMP.objects.filter(measure=measure).delete()
            
            # Create new MeasureVMP instances
            measure_vmps = []
            for row in result:
                vmp_id, vmp_type = row
                try:
                    vmp = VMP.objects.get(id=vmp_id)
                    
                    # Check for data based on measure's quantity_type
                    has_data = False
                    if measure.quantity_type == 'dose':
                        has_data = Dose.objects.filter(vmp=vmp).exclude(data=[]).exists()
                    else:  # ingredient
                        has_data = IngredientQuantity.objects.filter(vmp=vmp).exclude(data=[]).exists()
                    
                    if has_data:
                        measure_vmps.append(
                            MeasureVMP(
                                measure=measure,
                                vmp=vmp,
                                type=vmp_type
                            )
                        )
                except VMP.DoesNotExist:
                    self.stdout.write(
                        self.style.WARNING(f'VMP with id {vmp_id} does not exist')
                    )
                    continue
            
            # Bulk create all MeasureVMP instances
            if measure_vmps:
                MeasureVMP.objects.bulk_create(measure_vmps)
                self.stdout.write(
                    self.style.SUCCESS(
                        f'Successfully created {len(measure_vmps)} MeasureVMP instances for {measure_name}'
                    )
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f'No VMPs found for measure {measure_name}')
                )
