from django.core.management.base import BaseCommand
from django.db import transaction, connection
from viewer.models import Measure, VMP, MeasureVMP, Dose, IngredientQuantity, IndicativeCost, SCMDQuantity, DDDQuantity
from pathlib import Path


def execute_measure_sql(measure_slug):
    """
    Execute SQL for a measure from its vmps.sql file.
    
    Args:
        measure_slug: slug of the measure
    Returns:
        List of results or None if SQL file not found
    """
    try:
        measure = Measure.objects.get(slug=measure_slug)
    except Measure.DoesNotExist:
        raise ValueError(f"Measure '{measure_slug}' not found")

    sql_path = Path(__file__).parent.parent.parent / 'measures' / measure_slug / 'vmps.sql'
    if not sql_path.exists():
        return None
        
    with open(sql_path) as f:
        sql = f.read()

    with connection.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()

    return result or []


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

        result = execute_measure_sql(measure_slug)

        if result is None:
            self.stdout.write(
                self.style.WARNING(f'No SQL file found for measure {measure_slug} - skipping')
            )
            return

        with transaction.atomic():
            MeasureVMP.objects.filter(measure=measure).delete()
            
            measure_vmps = []
            for row in result:
                vmp_id, vmp_type = row
                try:
                    vmp = VMP.objects.get(id=vmp_id)
                    
                    measure_vmps.append(
                        MeasureVMP(
                            measure=measure,
                            vmp=vmp,
                            type=vmp_type
                        )
                    )
                except VMP.DoesNotExist:
                    self.stdout.write(
                        self.style.WARNING(
                            f'VMP with id {vmp_id} does not exist'
                        )
                    )
                    continue

            if measure_vmps:
                MeasureVMP.objects.bulk_create(measure_vmps)
                self.stdout.write(
                    self.style.SUCCESS(
                        f'Successfully created {len(measure_vmps)} MeasureVMP instances for {measure_slug}'
                    )
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f'No VMPs found for measure {measure_slug}')
                )
