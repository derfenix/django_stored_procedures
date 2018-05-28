import os
import re
from functools import partial
from itertools import chain
from typing import Callable, Dict, List, Optional, Tuple, TypeVar, Union

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
            if os.access(d, os.R_OK | os.X_OK):
                files = os.listdir(d)
                sp_list += [os.path.join(d, f) for f in files if f.endswith('.sql')]

        if self._extra_files is not None:
            sp_list += self._extra_files
        
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

    def _execute_sp(self, *args, name: str, ret='one', **kwargs):
        """
        Execute stored procedure and return result 
        
        :param name: 
        :param args: 
        :param ret: One of 'one', 'all', 'cursor' or number
        """
        args = [arg for arg in args if arg is not None]

        arguments = ",".join(chain(
            ['%s' for _ in args],
            ["{} := {}".format(name, value) for name, value in kwargs.items()]
        ))
        # noinspection SqlDialectInspection, SqlNoDataSourceInspection
        statement = "SELECT * FROM {name}({arguments})".format(
            name=name, arguments=arguments,
        )

        return self._get_res(statement, args, ret)

    def _execute_view(self, filters: Optional[str] = None, params: Optional[List] = None, *,
                      name: str, ret: str = 'one', fields: str = '*'):
        """
        Select from view and return result 

        :param name: 
        :param filters: 
        :param ret: One of 'one', 'all', 'cursor' or number
        """
        if filters is not None:
            filters = filters.strip()

        # noinspection SqlDialectInspection, SqlNoDataSourceInspection
        statement = "SELECT {fields} FROM {name}{where}{filters}".format(
            name=name, filters=filters if filters else '',
            where=' WHERE ' if filters else '',
            fields=fields
        )

        return self._get_res(statement, params, ret)

    def _get_res(self, statement: str, args: List, ret: Union[str, int]) -> Union[List, Dict, Cursor]:
        if not isinstance(ret, int):
            assert ret in ['one', 'all', 'cursor']
        cursor = self.connection.cursor()
        try:
            cursor.execute(statement, args)
            if ret == 'cursor':
                return cursor

            columns = self.columns_from_cursor(cursor)
            if len(columns) > 0:
                if ret == 'one':
                    res = self.row_to_dict(cursor.fetchone(), columns)
                elif ret == 'all':
                    res = [self.row_to_dict(row, columns) for row in cursor]
                else:
                    res = [self.row_to_dict(row, columns) for row in cursor.fetchmany(ret)]
            else:
                if ret == 'one':
                    res = cursor.fetchone()
                elif ret == 'all':
                    res = [row for row in cursor]
                else:
                    res = [row for row in cursor.fetchmany(ret)]
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
