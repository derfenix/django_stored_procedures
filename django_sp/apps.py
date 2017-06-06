from django.apps import AppConfig

from django_sp.loader import Loader
from . import init_loader, logger as base_logger

logger = base_logger.getChild(__name__)


class DjangoSPConfig(AppConfig):
    name = 'django_sp'
    verbose_name = "Django Stored Procedures"

    def ready(self):
        loader = Loader()
        init_loader(loader)
        logger.debug('Application configured')
