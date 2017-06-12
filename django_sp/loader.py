import os
import re
from functools import partial
from typing import Callable, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

from django.apps import apps
from django.conf import settings
from django.db import connection

from . import logger as base_logger

logger = base_logger.getChild(__name__)

Cursor = TypeVar('Cursor')


class Loader:
    REGEXP = re.compile(r'CREATE (?:OR REPLACE)? (?P<type>(?:VIEW)|(?:FUNCTION)) (?P<name>[^_]\w+)', re.MULTILINE)
    EXECUTORS = {
        'function': '_execute_sp',
        'view': '_execute_view'
    }

    def __init__(self, extra_files: Optional[List] = None):
        self._sp_list = []
        self._sp_names = None
        self._connection = None
        self._extra_files = extra_files

        self._fill_sp_files_list()
        self.populate_helper()

    @property
    def connection(self):
        if self._connection is None:
            self._connection = connection
        return self._connection

    def _fill_sp_files_list(self):
        sp_dir = getattr(settings, 'SP_DIR', 'sp/')
        sp_list = []
        for name, app in apps.app_configs.items():
            app_path = app.path
            d = os.path.join(app_path, sp_dir)
            logger.debug('Looking into %s app at %s', name, d)
            if os.access(d, os.R_OK | os.X_OK):
                logger.debug('Added sp dir for %s', name)
                files = os.listdir(d)
                logger.debug('Added files to sp_list: %s', files)
                sp_list += [os.path.join(d, f) for f in files if f.endswith('.sql')]
            else:
                logger.error('Directory %s/%s not readable!', app_path, d)

        if self._extra_files is not None:
            sp_list += self._extra_files
            logger.debug("Added extra files: %s", self._extra_files)
        
        self._sp_list = sp_list

    def _check_file_for_reading(self, sp_file: str) -> bool:
        if not os.access(sp_file, os.R_OK):
            logger.error('File {} not readable! Can\'t install stored procedure from it'.format(sp_file))
            self._sp_list.remove(sp_file)
            return False
        return True

    def load_sp_into_db(self):
        with self.connection.cursor() as cursor:
            for sp_file in self._sp_list[:]:
                if not self._check_file_for_reading(sp_file):
                    continue
                with open(sp_file, 'r') as f:
                    cursor.execute(f.read())

    def add_to_list(self, file_path: str):
        self._sp_list.append(file_path)

    def populate_helper(self):
        self._sp_names = {}
        for sp_file in self._sp_list:
            if not self._check_file_for_reading(sp_file):
                continue
            with open(sp_file, 'r') as f:
                names = self.REGEXP.findall(f.read())
                for typ, name in names:
                    self._sp_names[name] = typ.lower()

    def _execute_sp(self, *args, name: str, ret='one') -> Union[List, Iterator, Cursor]:
        """
        Execute stored procedure and return result 
        
        :param name: 
        :param args: 
        :param ret: One of 'one', 'all' or 'cursor'  
        """
        assert ret in ['one', 'all', 'cursor']

        placeholders = ",".join(['%s' for _ in range(len(args))])
        # noinspection SqlDialectInspection, SqlNoDataSourceInspection
        statement = "SELECT * FROM {name}({placeholders})".format(
            name=name, placeholders=placeholders,
        )

        cursor = self.connection.cursor()
        try:
            cursor.execute(statement, args)
            if ret == 'cursor':
                return cursor

            if ret == 'one':
                res = cursor.fetchone()
            else:  # ret == 'all'
                res = (row for row in cursor)
        finally:
            if ret != 'cursor':
                cursor.close()

        return res

    def _execute_view(self, filters: Optional[str] = None, params: Optional[List] = None, *,
                      name: str, ret: str = 'one'):
        """
        Select from view and return result 

        :param name: 
        :param filters: 
        :param ret: One of 'one', 'all' or 'cursor'  
        """
        assert ret in ['one', 'all', 'cursor']

        # noinspection SqlDialectInspection, SqlNoDataSourceInspection
        statement = "SELECT * FROM {name} {where} {filters}".format(
            name=name, filters=filters if filters is not None else '',
            where='WHERE' if filters is not None else ''
        )

        cursor = self.connection.cursor()
        try:
            cursor.execute(statement, params)
            if ret == 'cursor':
                return cursor

            columns = self.columns_from_cursor(cursor)
            if ret == 'one':
                res = self.row_to_dict(cursor.fetchone(), columns)
            else:  # ret == 'all'
                res = [self.row_to_dict(row, columns) for row in cursor]
        finally:
            if ret != 'cursor':
                cursor.close()

        return res

    @staticmethod
    def columns_from_cursor(cursor: Cursor) -> List:
        return [col[0] for col in cursor.description]

    @staticmethod
    def row_to_dict(row: Tuple, columns: List) -> Optional[Dict]:
        if row:
            return dict(zip(columns, row))
        else:
            return None

    def __getitem__(self, item: str) -> Callable:
        if item not in self._sp_names.keys():
            raise KeyError("Stored procedure {} not found".format(item))

        executor = self.EXECUTORS[self._sp_names[item]]
        func = partial(getattr(self, executor), name=item)
        return func

    def __getattr__(self, item: str) -> Union[Callable, object]:
        if item in self._sp_names:
            return self.__getitem__(item)

        return self.__getattribute__(item)

    def __len__(self) -> int:
        return len(self._sp_names)

    def __contains__(self, item: str) -> bool:
        return item in self._sp_names

    def list(self) -> Tuple:
        return tuple(self._sp_names.keys())

    def commit(self):
        self.connection.commit()
