import logging

logger = logging.getLogger('django_sp')

__all__ = ['sp_loader']

# Initiated in django_sp.apps.DjangoSPConfig.ready()
sp_loader = None
""":type: django_sp.loader.Loader"""


def init_loader(loader):
    global sp_loader
    sp_loader = loader
