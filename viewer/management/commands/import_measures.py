import yaml
from pathlib import Path
from django.core.management.base import BaseCommand
from django.utils.text import slugify
from viewer.models import Measure, MeasureTag, MeasureAnnotation
from schema import Schema, And, Optional, SchemaError, Or
from datetime import datetime, timedelta, date

class Command(BaseCommand):
    help = 'Import measures from YAML definition files'

    def add_arguments(self, parser):
        parser.add_argument('folder_name', nargs='?', type=str, help='Optional specific measure folder to import')

    def handle(self, *args, **kwargs):
        measures_dir = Path(__file__).parent.parent.parent / 'measures'
        folder_name = kwargs.get('folder_name')
        
        if folder_name:
            if folder_name != slugify(folder_name):
                self.stdout.write(
                    self.style.ERROR(f'Invalid folder name: {folder_name}. Must be a valid slug (lowercase letters, numbers, and hyphens only)')
                )
                return
                
            measure_dirs = [measures_dir / folder_name]
            if not measure_dirs[0].exists():
                self.stdout.write(
                    self.style.ERROR(f'Measure folder not found: {folder_name}')
                )
                return
        else:
            measure_dirs = [d for d in measures_dir.glob('*/') if d.is_dir()]
        
        for measure_dir in measure_dirs:
            yaml_file = measure_dir / 'definition.yaml'
            if not yaml_file.exists():
                self.stdout.write(
                    self.style.WARNING(f'No definition.yaml found in {measure_dir}')
                )
                continue
                            
            try:
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
            except yaml.YAMLError as e:
                self.stdout.write(
                    self.style.ERROR(f'Invalid YAML in {yaml_file}: {str(e)}')
                )
                continue
                
            try:
                validate_measure_yaml(data)
                tags = data.get('tags', [])
                tag_objects = validate_measure_tags(tags)
                
            except (ValueError, SchemaError) as e:
                self.stdout.write(
                    self.style.ERROR(f'Invalid measure definition in {yaml_file}: {str(e)}')
                )
                continue
                
            measure, created = Measure.objects.update_or_create(
                slug=data.get('slug', measure_dir.name),
                defaults={
                    'name': data['name'],
                    'short_name': data['short_name'],
                    'description': data['description'],
                    'why_it_matters': data['why_it_matters'],
                    'how_is_it_calculated': data['how_is_it_calculated'],
                    'quantity_type': data.get('quantity_type', 'dose'),
                    'authored_by': data.get('authored_by', None),
                    'checked_by': data.get('checked_by', None),
                    'date_reviewed': data.get('date_reviewed', None),
                    'next_review': data.get('next_review', None),
                    'first_published': data.get('first_published', None),
                    'status': data.get('status', 'in_development'),
                    'default_view_mode': data.get('default_view_mode', 'percentiles')
                }
            )
            
            measure.tags.set(tag_objects)
            
            self._handle_annotations(measure, data.get('annotations', []))
            
            action = 'Created' if created else 'Updated'
            self.stdout.write(
                self.style.SUCCESS(f'{action} measure: {measure.name} ({measure.slug})')
            )

    def _handle_annotations(self, measure, annotations_data):
        MeasureAnnotation.objects.filter(measure=measure).delete()
        
        for annotation_data in annotations_data:
            try:
                if isinstance(annotation_data['date'], str):
                    date_obj = datetime.strptime(annotation_data['date'], '%Y-%m-%d').date()
                else:
                    date_obj = annotation_data['date']
                
                MeasureAnnotation.objects.create(
                    measure=measure,
                    date=date_obj,
                    label=annotation_data['label'],
                    description=annotation_data.get('description', ''),
                    colour=annotation_data.get('colour', '#DC3220'),
                )
            except (KeyError, ValueError) as e:
                self.stdout.write(
                    self.style.WARNING(f'Invalid annotation data for {measure.slug}: {str(e)}')
                )

def validate_date_format(date_val):
    if date_val == '':
        return True
    if isinstance(date_val, date):
        return True
    if isinstance(date_val, str):
        try:
            datetime.strptime(date_val, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    return False

def validate_review_dates(data):
    # Skip validation if either field is missing
    
    if 'date_reviewed' not in data or 'next_review' not in data:
        return True
    
    # Skip validation if either field is empty
    if not data.get('date_reviewed') or not data.get('next_review'):
        return True
        
    try:
        def to_datetime(val):
            if isinstance(val, date):
                return datetime.combine(val, datetime.min.time())
            return datetime.strptime(val, '%Y-%m-%d')

        reviewed = to_datetime(data['date_reviewed'])
        next_review = to_datetime(data['next_review'])
        
        if next_review <= reviewed:
            raise ValueError("next_review must be after date_reviewed")
            
        # should review within 1 year
        now = datetime.now()
        one_year = timedelta(days=365)
        
        if reviewed < now - one_year or reviewed > now + one_year:
            raise ValueError("date_reviewed must be within ±1 year of current date")
            
        if next_review < now - one_year or next_review > now + one_year:
            raise ValueError("next_review must be within ±1 year of current date")
            
        return True
    except ValueError as e:
        return str(e)

def validate_measure_yaml(data):
    schema = Schema({
        'name': And(str),
        'short_name': And(str),
        'description': And(str),
        'why_it_matters': And(str),
        'how_is_it_calculated': And(str),
        'tags': And(list, lambda tags: all(isinstance(t, str) for t in tags), 
                 lambda tags: len(tags) >= 1,
                 error='Tags must be a non-empty list of strings'),
        'quantity_type': And(str, lambda qt: qt in ['scmd', 'dose', 'ingredient', 'ddd', 'indicative_cost'],
                           error='quantity_type must be one of: scmd, dose, ingredient, ddd, indicative_cost'),
        Optional('authored_by'): And(str),
        Optional('checked_by'): And(str),
        Optional('date_reviewed'): And(
            Or(str, date), 
            validate_date_format,
            error='date_reviewed must be in format YYYY-MM-DD or a valid date object'
        ),
        Optional('next_review'): And(
            Or(str, date), 
            validate_date_format,
            error='next_review must be in format YYYY-MM-DD or a valid date object'
        ),
        Optional('first_published'): And(
            Or(str, date), 
            validate_date_format,
            error='first_published must be in format YYYY-MM-DD or a valid date object'
        ),
        Optional('status'): And(
            str,
            lambda status: status in [choice[0] for choice in Measure.STATUS_CHOICES],
            error='status must be one of: in_development, preview, published'
        ),
        Optional('default_view_mode'): And(
            str,
            lambda mode: mode in [choice[0] for choice in Measure.VIEW_MODE_CHOICES],
            error='default_view_mode must be one of: percentiles, icb, region, national'
        ),
        Optional('annotations'): And(
            list,
            lambda annotations: all(isinstance(a, dict) for a in annotations),
            error='annotations must be a list of dictionaries'
        )
    })
    
    schema.validate(data)
    
    date_validation = validate_review_dates(data)
    if date_validation is not True:
        raise SchemaError(date_validation)

MEASURE_TAG_DEFINITIONS = {
    'Safety': {
        'description': 'This measure supports medicines safety work to reduce risk and minimise mistakes in hospitals.',
        'colour': '#F44336'
    },
    'Antimicrobial stewardship': {
        'description': 'This measure supports work to promote antimicrobial stewardship in hospitals.',
        'colour': '#009688'
    },
    'Low value prescribing': {
        'description': 'This measure supports work to reduce low value prescribing in hospitals.',
        'colour': '#9C27B0'
    },
    'Value': {
        'description': 'This measure supports cost saving work in hospitals.',
        'colour': '#2196F3'
    },
    'Greener NHS': {
        'description': 'This measure supports work to reduce the impact of medicines issued in hospitals on the overall environmental impact of the NHS.',
        'colour': '#4CAF50'
    },
    'Efficiency': {
        'description': 'This measure supports work to improve efficiency in hospitals.',
        'colour': '#FF9800'
    }
}


def validate_measure_tags(tags):
    """Validate and create measure tags if they don't exist."""
    existing_tags = set(MeasureTag.objects.values_list('name', flat=True))
    missing_tags = [tag for tag in tags if tag not in existing_tags]
    
    if missing_tags:
        for tag_name in missing_tags:
            if tag_name in MEASURE_TAG_DEFINITIONS:
                tag_def = MEASURE_TAG_DEFINITIONS[tag_name]
                MeasureTag.objects.create(
                    name=tag_name,
                    description=tag_def['description'],
                    colour=tag_def['colour']
                )
                   
    return MeasureTag.objects.filter(name__in=tags) 