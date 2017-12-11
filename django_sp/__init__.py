import logging

logger = logging.getLogger('django_sp')

__all__ = ['sp_loader']


class SPLoader:
    __slots__ = ['_loader']

    def __init__(self):
        self._loader = None

    def __call__(self, *args, **kwargs):
        if self._loader is None:
            from .loader import Loader
            self._loader = Loader()
        return self._loader


sp_loader = SPLoader()
