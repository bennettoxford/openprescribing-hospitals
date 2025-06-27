from django.core.management.base import BaseCommand
from pipeline.utils.maintenance import (
    enable_maintenance_mode, 
    disable_maintenance_mode, 
    get_maintenance_status,
)

class Command(BaseCommand):
    help = 'Manage maintenance mode for data-dependent pages'

    def add_arguments(self, parser):
        parser.add_argument(
            'action',
            choices=['enable', 'disable', 'status'],
            help='Action to perform'
        )

    def handle(self, *args, **options):
        action = options['action']

        if action == 'enable':
            if enable_maintenance_mode():
                self.stdout.write(
                    self.style.SUCCESS('Maintenance mode enabled')
                )
            else:
                self.stdout.write(
                    self.style.ERROR('Failed to enable maintenance mode')
                )

        elif action == 'disable':
            if disable_maintenance_mode():
                self.stdout.write(
                    self.style.SUCCESS('Maintenance mode disabled')
                )
            else:
                self.stdout.write(
                    self.style.ERROR('Failed to disable maintenance mode')
                )

        elif action == 'status':
            status = get_maintenance_status()
            if status.get('error'):
                self.stdout.write(
                    self.style.ERROR(f"Error getting status: {status['error']}")
                )
            elif status['enabled']:
                self.stdout.write(
                    self.style.WARNING('Maintenance mode is ENABLED')
                )
                if status.get('duration'):
                    self.stdout.write(f"Running for: {status['duration']}")
            else:
                self.stdout.write(
                    self.style.SUCCESS('Maintenance mode is DISABLED')
                )
