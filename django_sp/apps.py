from django.apps import AppConfig

import django_sp
from django_sp.loader import Loader
from . import logger as base_logger

logger = base_logger.getChild(__name__)


class DjangoSPConfig(AppConfig):
    name = 'django_sp'
    verbose_name = "Django Stored Procedures"

    def ready(self):
        django_sp.sp_loader = Loader()
