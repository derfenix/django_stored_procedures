import os
import re
from functools import partial

import logging
from django.apps import apps
from django.conf import settings
from django.db import connections

logger = logging.getLogger('django_sp.loader')

# TODO: Extend REGEXP and executor to validate arguments


class Loader:
    REGEXP = re.compile('CREATE OR REPLACE FUNCTION (\w+)')

    def __init__(self, db_name='default'):
        self._db_name = db_name
        self._sp_list = []
        self._names = []

        self._get_sp_list()
        self._load_sp_into_db()
        self._populate_helper()

    def _get_sp_list(self):
        sp_dir = settings.get('SP_DIR', 'sp')
        apps_list = settings.get('INSTALLED_APPS')
        sp_list = []
        for app in apps_list:
            app_path = apps.get_app_config(app).path
            d = os.path.join(app_path, sp_dir)
            if os.access(d, os.R_OK & os.X_OK):
                content = os.listdir(d)
                sp_list += [os.path.join(d, f) for f in content]

        self._sp_list = sp_list

    def _load_sp_into_db(self):
        cursor = connections[self._db_name].cursor()
        for sp_file in self._sp_list[:]:
            if not os.access(sp_file, os.R_OK):
                logger.error('File {} not readable! Can\'t install stored procedure from it'.format(sp_file))
                self._sp_list.pop(sp_file)
                continue
            with open(sp_file, 'r') as f:
                cursor.execute(f.read())

    def _populate_helper(self):
        names = []
        for sp_file in self._sp_list:
            with open(sp_file, 'r') as f:
                names += self.REGEXP.findall(f.read())
        self._names = names

    def _execute_sp(self, name, *args, fetchone=False):
        cursor = connections[self._db_name].cursor()
        statement = "SELECT {}({})".format(
            name, ", ".join(args)
        )
        cursor.execute(statement)
        columns = [col[0] for col in cursor.description]
        method = cursor.fetchall if not fetchone else cursor.fetchone
        return [dict(zip(columns, row)) for row in method()]

    def __getitem__(self, item):
        if item not in self._names:
            raise KeyError("Stored procedure {} not found".format(item))
        return partial(self._execute_sp, name=item)

    def __getattr__(self, item):
        if item in self._names:
            return self.__getitem__(item)

        return self.__getattribute__(item)

    def list(self):
        return self._sp_list
