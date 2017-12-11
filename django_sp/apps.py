from django.apps import AppConfig

from . import logger as base_logger

logger = base_logger.getChild(__name__)


class DjangoSPConfig(AppConfig):
    name = 'django_sp'
    verbose_name = "Django Stored Procedures"
