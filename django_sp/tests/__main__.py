import sys

import django
from django.conf import settings
from django.test.runner import DiscoverRunner

if __name__ == '__main__':
    if not settings.configured:
        settings.configure(
            INSTALLED_APPS=['django_sp.apps.DjangoSPConfig'],
            DATABASES={
                'default': {
                    'ENGINE': 'django.db.backends.postgresql',
                    'HOST': '127.0.0.1',
                    'NAME': 'postgres',
                    'USER': 'postgres'
                }
            },
            SP_DIR='tests/'
        )
        django.setup()

    test_runner = DiscoverRunner(verbosity=1)
    failures = test_runner.run_tests(['django_sp'])
    if failures:
        sys.exit(failures)
