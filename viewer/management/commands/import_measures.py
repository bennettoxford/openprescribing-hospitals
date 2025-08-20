import yaml
from pathlib import Path
from django.core.management.base import BaseCommand
from django.utils.text import slugify
from viewer.models import Measure, MeasureTag
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
                validate_measure_tags(data['tags'])
            except ValueError as e:
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
                    'status': data.get('status', 'in_development')
                }
            )
            
            tags = data.get('tags', [])
            tag_objects = validate_measure_tags(tags)
            measure.tags.set(tag_objects)
            
            if created:
                self.stdout.write(
                    self.style.SUCCESS(f'Created measure: {measure.name}')
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS(f'Updated measure: {measure.name}')
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
        )
    })
    
    schema.validate(data)
    
    date_validation = validate_review_dates(data)
    if date_validation is not True:
        raise SchemaError(date_validation)

def validate_measure_tags(tags):
    """Validate that all specified tags exist in the database."""
    existing_tags = set(MeasureTag.objects.values_list('name', flat=True))
    invalid_tags = [tag for tag in tags if tag not in existing_tags]
    
    if invalid_tags:
        raise ValueError(
            f"The following tags do not exist in the database: {', '.join(invalid_tags)}"
        )
    return MeasureTag.objects.filter(name__in=tags) 