import logging
import os
import re
from functools import partial

import typing
from django.apps import apps
from django.conf import settings
from django.db import connections

logger = logging.getLogger('django_sp.loader')

Cursor = typing.TypeVar('Cursor')

# TODO: Extend REGEXP and executor to validate arguments
# TODO: Add support for multiple databases
# TODO: Add management command and move there procedures installation

EXECUTE_PARTIAL_DOC = """
Call the procedure and return dict or list of dicts with procedure's return value.

:param list args: List of arguments for the procedure
:param bool fetchone: Get all values or just one. If true - one value will be returned, all other will be lost.
"""


class Loader:
    REGEXP = re.compile('CREATE OR REPLACE FUNCTION (\w+)', re.MULTILINE)

    def __init__(self, db_name='default'):
        self._db_name = db_name
        self._sp_list = []
        self._sp_names = []
        self._connection = connections[self._db_name]

        self._get_sp_files_list()
        self.populate_helper()

    def _get_sp_files_list(self):
        sp_dir = settings.get('SP_DIR', 'sp')
        apps_list = settings.get('INSTALLED_APPS')
        sp_list = []
        for app in apps_list:
            app_path = apps.get_app_config(app).path
            d = os.path.join(app_path, sp_dir)
            if os.access(d, os.R_OK | os.X_OK):
                content = os.listdir(d)
                sp_list += [os.path.join(d, f) for f in content]
            else:
                logger.error('Directory {} not readable! Can\'t install stored procedures from there'.format(d))

        self._sp_list = sp_list

    def load_sp_into_db(self):
        with self._connection.cursor() as cursor:
            for sp_file in self._sp_list[:]:
                if not os.access(sp_file, os.R_OK):
                    logger.error('File {} not readable! Can\'t install stored procedure from it'.format(sp_file))
                    self._sp_list.pop(sp_file)
                    continue
                with open(sp_file, 'r') as f:
                    cursor.execute(f.read())

    def populate_helper(self):
        names = []
        for sp_file in self._sp_list:
            with open(sp_file, 'r') as f:
                names += self.REGEXP.findall(f.read())
        self._sp_names = names

    def _execute_sp(self, name: str, *args, fetchone=True, cursor_return=False) -> typing.Union[list, dict, Cursor]:
        placeholders = ",".join(['%s' for _ in range(len(args))])
        statement = "SELECT {name}({placeholders})".format(
            name=name, placeholders=placeholders,
        )

        cursor = self._connection.cursor()
        try:
            cursor.execute(statement, args)
            if cursor_return:
                return cursor

            columns = [col[0] for col in cursor.description]
            if fetchone:
                res = dict(zip(columns, cursor.fetchone()))
            else:
                res = [dict(zip(columns, row)) for row in cursor]
        finally:
            if not cursor_return:
                cursor.close()

        return res

    def __getitem__(self, item: str) -> typing.Callable:
        if item not in self._sp_names:
            raise KeyError("Stored procedure {} not found".format(item))
        func = partial(self._execute_sp, name=item)
        func.__doc__ = EXECUTE_PARTIAL_DOC
        return func

    def __getattr__(self, item: str) -> typing.Union[typing.Callable, object]:
        if item in self._sp_names:
            return self.__getitem__(item)

        return self.__getattribute__(item)

    def list(self) -> typing.List:
        return self._sp_list

    def commit(self):
        self._connection.commit()
