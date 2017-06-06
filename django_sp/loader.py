import os
import re
import typing
from functools import partial

from django.apps import apps
from django.conf import settings
from django.db import connections

from . import logger as base_logger

logger = base_logger.getChild(__name__)

Cursor = typing.TypeVar('Cursor')

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

        self._fill_sp_files_list()
        self.populate_helper()

    def _fill_sp_files_list(self):
        sp_dir = getattr(settings, 'SP_DIR', '/sp/')
        sp_list = []
        for name, app in apps.app_configs.items():
            logger.debug('Looking into %s app', name)
            app_path = app.path
            d = os.path.join(app_path, sp_dir)
            if os.access(d, os.R_OK | os.X_OK):
                logger.debug('Added sp dir for %s', name)
                files = os.listdir(d)
                logger.debug('Added files to sp_list: %s', files)
                sp_list += [os.path.join(d, f) for f in files]
            else:
                logger.error('Directory %s/%s not readable!', app_path, d)

        self._sp_list = sp_list

    def _check_file_for_reading(self, sp_file: str) -> bool:
        if not os.access(sp_file, os.R_OK):
            logger.error('File {} not readable! Can\'t install stored procedure from it'.format(sp_file))
            self._sp_list.remove(sp_file)
            return False
        return True

    def load_sp_into_db(self):
        with self._connection.cursor() as cursor:
            for sp_file in self._sp_list[:]:
                if not self._check_file_for_reading(sp_file):
                    continue
                with open(sp_file, 'r') as f:
                    cursor.execute(f.read())

    def populate_helper(self):
        names = []
        for sp_file in self._sp_list:
            if not self._check_file_for_reading(sp_file):
                continue
            with open(sp_file, 'r') as f:
                names += self.REGEXP.findall(f.read())
        self._sp_names = names

    def _execute_sp(self, name: str, *args, ret='one') -> typing.Union[typing.List, typing.Iterator, Cursor]:
        """
        Execute stored procedure and return result 
        
        :param name: 
        :param args: 
        :param ret: One of 'one', 'all' or 'cursor'  
        """
        assert ret in ['one', 'all', 'cursor']

        placeholders = ",".join(['%s' for _ in range(len(args))])
        statement = "SELECT {name}({placeholders})".format(
            name=name, placeholders=placeholders,
        )

        cursor = self._connection.cursor()
        try:
            cursor.execute(statement, args)
            if ret == 'cursor':
                return cursor

            columns = [col[0] for col in cursor.description]
            if ret == 'one':
                res = dict(zip(columns, cursor.fetchone()))
            else:  # ret == 'all'
                res = (dict(zip(columns, row)) for row in cursor)
        finally:
            if ret != 'cursor':
                cursor.close()

        return res

    @staticmethod
    def columns_from_cursor(cursor: Cursor) -> typing.List:
        return [col[0] for col in cursor.description]

    @staticmethod
    def row_to_dict(row: typing.Tuple, columns: typing.List) -> typing.Dict:
        return dict(zip(columns, row))

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

    def __len__(self) -> int:
        return len(self._sp_names)

    def __contains__(self, item: str) -> bool:
        return item in self._sp_names

    def list(self) -> typing.List:
        return self._sp_names

    def commit(self):
        self._connection.commit()
