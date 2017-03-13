__all__ = ['sp_loader']

default_app_config = 'django_sp.apps.DjangoSPConfig'

# Initiated in django_sp.apps.DjangoSPConfig.ready()
sp_loader = None
""":type: django_sp.loader.Loader"""
