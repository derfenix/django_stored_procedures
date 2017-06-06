from django.core.management import BaseCommand

from django_sp.loader import Loader


class Command(BaseCommand):
    help = 'Load stored procedures and other database stuff'

    def handle(self, *args, **options):
        loader = Loader()
        loader.load_sp_into_db()
        print("Available {} procedures:", len(loader))
