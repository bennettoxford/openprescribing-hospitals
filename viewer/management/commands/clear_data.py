from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = 'Clear data from the database while keeping the table structures'

    def add_arguments(self, parser):
        parser.add_argument('models', nargs='+', type=str, help='List of model names to clear')

    def handle(self, *args, **kwargs):
        model_names = kwargs['models']
        
        for model_name in model_names:
            try:
                model = apps.get_model('viewer', model_name)
                self.stdout.write(f'Clearing data from {model_name}...')
                model.objects.all().delete()
                self.stdout.write(self.style.SUCCESS(f'Successfully cleared data from {model_name}'))
            except LookupError:
                self.stdout.write(self.style.WARNING(f'Model {model_name} not found'))

        self.stdout.write(self.style.SUCCESS('All data has been cleared from the database'))

# python manage.py clear_data VTM VMP Ingredient