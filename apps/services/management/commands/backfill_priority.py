import time

from django.core.management.base import BaseCommand
from apps.services.models import Service


BATCH_SIZE = 10000


class Command(BaseCommand):
    help = 'Backfills the Service.priority field for all services where it is NULL.'

    def handle(self, *args, **options):
        self.stdout.write('Starting priority backfill...')

        queryset = Service.objects.filter(priority__isnull=True)

        while queryset.exists():
            batch_pks = list(queryset.values_list('pk', flat=True)[:BATCH_SIZE])

            Service.objects.filter(pk__in=batch_pks).update(priority='MEDIUM')

            self.stdout.write(f"Updated {len(batch_pks)} services...")
            
            time.sleep(0.5)
        
        self.stdout.write('Priority backfill complete!')




    