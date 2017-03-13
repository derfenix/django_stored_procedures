from django.apps import AppConfig

import django_sp
from django_sp.loader import Loader


class DjangoSPConfig(AppConfig):
    name = 'django_sp'
    verbose_name = "Django Stored Procedures"

    def ready(self):
        loader = Loader()
        django_sp.sp_loader = loader
