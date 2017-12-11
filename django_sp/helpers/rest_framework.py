import datetime
import re
from collections import OrderedDict, defaultdict
from decimal import Decimal
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union

from django.core.exceptions import ValidationError
from django.utils.dateparse import parse_datetime
from django.utils.functional import cached_property
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.utils.urls import replace_query_param, remove_query_param

from django_sp import sp_loader
from . import logger as base_logger

__all__ = ['RawSQLFilterSet', 'RawSQLFilter', 'StringFilter', 'IntegerFilter', 'DecimalFilter', 'DateTimeFilter']

logger = base_logger.getChild(__name__)


def plain(value):
    return value


class NoValue:
    __slots__ = ()


novalue = NoValue()


def get_declared_filters(attrs, bases) -> Dict[str, Any]:
    filters = []
    for filter_name, obj in list(attrs.items()):
        if isinstance(obj, RawSQLFilter):
            obj = attrs.pop(filter_name)
            filters.append((filter_name, obj))

    if bases:
        for base in bases[::-1]:
            if hasattr(base, 'filters'):
                filters = list(base.filters.items()) + filters

    return OrderedDict(filters)


class RawSQLFilterSetOptions:
    __slots__ = ('order_by', 'logical_or')

    def __init__(self, options=None):
        self.order_by = getattr(options, 'order_by', False)
        self.logical_or = getattr(options, 'logical_or', [])


class RawSQLFilterMeta(type):
    # noinspection PyInitNewSignature
    def __new__(mcs, name, bases, attrs):
        try:
            parents = [b for b in bases if issubclass(b, RawSQLFilterSet)]
        except NameError:
            parents = None

        filters = get_declared_filters(attrs, parents)
        new_class = super(RawSQLFilterMeta, mcs).__new__(mcs, name, bases, attrs)

        if not parents:
            return new_class

        new_class._meta = RawSQLFilterSetOptions(getattr(new_class, 'Meta', None))
        new_class.filters = filters
        return new_class


class RawSQLFilter:
    """Base Raw SQL filter field"""
    OPERATORS_MAPPING = {
        'gte': '>=',
        'gt': '>',
        'lte': '<=',
        'lt': '<',
        'exact': '=',
        'isnull': '_isnull_condition_replace',
    }
    _converter = plain
    default = None

    def __init__(self, map_to: Optional[str] = None, default: Optional[Any] = None,
                 converter: Optional[Callable] = None):
        self.default = default
        self._map_to = map_to
        if converter is not None:
            self._converter = converter

        self._filter_set = None

    def set_filterset(self, filter_set: 'RawSQLFilterSet'):
        self._filter_set = filter_set

    @staticmethod
    def _isnull_condition_replace(value: str) -> [str, NoValue]:
        value = value.lower()
        if value == 'true':
            return 'IS NULL', novalue
        elif value == 'false':
            return 'IS NOT NULL', novalue
        else:
            raise ValueError('%s is not right value for `isnull` condition', value)

    def filter(self, name: str, condition: str, value: str) -> str:
        """
        Join field name, condition and value placeholder in one string
        
        If `map_to` was specified in field - its value will be used as field name.
        Value also passed through the `parse_value` method.
        
        Returns full sql condition such as 'field_name >= %s' and parsed value.
        """
        if self._map_to is not None:
            name = self._map_to

        sql_condition = self.OPERATORS_MAPPING.get(condition, condition)

        method = getattr(self, sql_condition, None)
        if method is not None and callable(method):
            sql_condition, value = method(value)

        sql = "{} {}{}".format(name, sql_condition, ' %s' if value is not novalue else '')
        self._filter_set.params = self._parse_value(value)

        return sql

    def _convert(self, value: Any) -> Any:
        try:
            # noinspection PyArgumentList
            return self._converter(value)
        except (TypeError, ValueError) as e:
            raise ValidationError("{} can not be converted to {} ({})".format(value, self._converter.__name__, e))

    def _validate(self, value: str):
        pass

    # noinspection PyMethodMayBeStatic
    def _parse_value(self, value: str) -> Any:
        """
        Prepare value to be stored in database
        
        This method can be overrided to convert or cleanup value
        """
        if value is not novalue:
            value = self._convert(value)
            self._validate(value)
        return value


class StringFilter(RawSQLFilter):
    _converter = str

    def __init__(self, max_length: int = 255, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_length = max_length

    def _parse_value(self, value: str) -> str:
        return super(StringFilter, self)._parse_value(value)[:self.max_length]


class IntegerFilter(RawSQLFilter):
    _converter = int

    def __init__(self, max_value: Union[int, Decimal, None] = None, min_value: Union[int, Decimal, None] = None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_value = max_value
        self.min_value = min_value

    def _validate(self, value: int):
        if self.max_value and value > self.max_value:
            raise ValidationError('Value can not be greater than {}'.format(self.max_value))
        if self.min_value and value < self.min_value:
            raise ValidationError('Value can not be less than {}'.format(self.min_value))


class DecimalFilter(IntegerFilter):
    _converter = Decimal


class DateTimeFilter(RawSQLFilter):
    _converter = datetime.datetime

    def __init__(self, input_format: Optional[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_format = input_format

    def _convert(self, value: Any):
        if self.input_format is not None:
            return datetime.datetime.strptime(value, self.input_format)
        else:
            return parse_datetime(value)


class CombinedSearchFilter(StringFilter):
    """Search within many fields"""
    value_templates = {
        'start': '%{}',
        'end': '{}%',
        'both': '%{}%'
    }

    # noinspection PyMissingConstructor
    def __init__(self, map_to: Tuple, max_length: Optional[int] = 255, strict_search: Optional[bool] = False,
                 case_sensitive: Optional[bool] = True, wildcard_place: Optional[str] = 'both',
                 strict_fields=None):
        """
        :param map_to: List of fields for search
        :param max_length: maximum value length
        :param strict_search: Search with equal (=) instead of ILIKE
        :param case_sensitive: Use LIKE instead of ILIKE
        :param wildcard_place: Where the ? mark should be placed
        :param strict_fields: Fields, that should not use LIKE/LIKE. Integer fields must be listed here, as well as
            other fields that have no LIKE/ILIKE operators
        """
        assert wildcard_place in ('start', 'end', 'both')
        self.search_fields = map_to
        self.strict_fields = strict_fields if strict_fields is not None else []
        self.wildcard_place = wildcard_place
        self.max_length = max_length
        self.strict_search = strict_search
        self.case_sensitive = case_sensitive

    def filter(self, name: str, condition: str, value: str) -> str:
        """
        Return query condition like::
            (field1 LIKE %s OR fields2 LIKE %s)

        Also append required number of parameters with `value` and % sign in place, specified by `wildcard_place`
        """
        conditions = []
        value_template = self.value_templates[self.wildcard_place]
        wildcarded_value = value_template.format(self._parse_value(value))

        for field in self.search_fields:
            if self.strict_search or field in self.strict_fields:
                operator = '='
                value_ = value
            else:
                value_ = wildcarded_value
                operator = 'LIKE' if self.case_sensitive else 'ILIKE'

            conditions.append('{field} {op} %s'.format(field=field, op=operator))
            self._filter_set.params = value_
        res = "({})".format(" OR ".join(conditions))
        return res


class RawSQLFilterSet(metaclass=RawSQLFilterMeta):
    """Filter base class for DRF - base for building raw-sql conditions"""
    # TODO: Support for multiple OR groups
    # TODO: Support for group by multiple fields

    ORDER_BY_RE = re.compile('^(?P<desc>-)?(?P<field>\w+)')

    def __init__(self, request=None):
        self._request = request
        self._request_filters = self._build_request_filters(request)
        self._params_values = []
        self._conditions_built = False

        for f in self.filters.values():
            f.set_filterset(self)

    def _build_request_filters(self, request) -> Dict[str, List[Tuple[str, Any]]]:
        """
        Build filters from GET params (DRF's `query_params`)
        
        Returns dict with filter names as keys and list of pairs (condition, value) as values.
        
        So it we got such request: ?age__gte=10&age__lt=60 
        we will have result such as {'age': [('gte', 10), ('lt': 60)]}, 
        """
        _request_filters = [
            self._get_filter_from_query_param(param, value)
            for param, value
            in request.query_params.items()
        ]

        request_filters = defaultdict(list)
        for param, condition, value in _request_filters:
            request_filters[str(param).lower()].append((condition, str(value)))

        return dict(request_filters)

    @staticmethod
    def _get_filter_from_query_param(param: str, value: Union[str, int]) -> Tuple[str, str, str]:
        """
        Split GET-param into name, condition and value
        
        Name and condition is the param's name splitted by __ (double underscore). 
        If not splitter exists in param's name - default condition (equal) will be used.
        """
        if '__' in param:
            param, condition = param.split('__')
            return param, condition, str(value)
        else:
            return param, '=', value

    @cached_property
    def sql(self) -> str:
        """
        Returns full sql conditions, that can be appended to any SELECT query
        
        Values for conditions are not present here, they are replaced by %s placeholders.
        Real values are availible at `params` property, only after querying this method. They are must be passed 
        as `params` argument for cursor's `execute` method to be escaped in right way.
        """
        and_cond = self._generate_conditions(
            ((name, filter_) for name, filter_ in self.filters.items() if name not in self._meta.logical_or)
        )

        or_cond = self._generate_conditions(
            ((name, filter_) for name, filter_ in self.filters.items() if name in self._meta.logical_or)
        )

        raw_sql = " AND ".join(and_cond)
        if self._meta.logical_or:
            raw_sql = "{raw_sql} AND ({or_})".format(raw_sql=raw_sql, or_=" OR ".join(or_cond) or 'TRUE')
        raw_sql = "{raw_sql} {order_by}".format(raw_sql=raw_sql, order_by=self._get_order_by())

        self._conditions_built = True
        return raw_sql

    @property
    def params(self) -> Tuple[Any, ...]:
        """Return tuple of params, that should be passed as replacemets for placeholders in query"""
        assert self._conditions_built, "`sql` method must be called before!"
        return tuple(self._params_values)

    @params.setter
    def params(self, value):
        if value is not novalue:
            self._params_values.append(value)

    def _get_order_by(self) -> str:
        """
        Generate 'ORDER BY ...' string, based on `Meta.order_by` value
        
        `-field_name` - for DESC ordering, `field_name` - for ASC ordering 
        """
        if not self._meta.order_by:
            return ''

        direction, field = self.ORDER_BY_RE.search(self._meta.order_by).groups()
        return "ORDER BY {field} {direction}".format(
            field=field, direction='ASC' if direction is None else 'DESC'
        )

    def _generate_conditions(self,
                             filters: Generator[Tuple[str, RawSQLFilter], None, None]) -> Generator[str, None, None]:
        """
        Returns generator, yields raw-sql conditions strings
         
        E.g. 'field_name >= %s`
        
        :param filters: Generator with filter's name and `RawSQLFilter` instance
        """
        for name, filter_ in filters:
            conds_and_values = self._request_filters.get(name)
            if conds_and_values:
                for condition, value in conds_and_values:
                    try:
                        sql = filter_.filter(name, condition, value)
                    except ValidationError as e:
                        raise ValidationError('Exception raised for {}: {}'.format(name, e))
                    yield sql
            elif filter_.default is not None:
                self.params = filter_.default
                yield "{} = %s".format(name)


class PageNumberPaginator:
    default_page_size = 50
    page_size_param = 'page_size'
    page_number_param = 'page'

    def __init__(self, cursor, request: Request):
        self.cursor = cursor
        self.request = request

    @cached_property
    def page(self) -> int:
        return int(self.request.query_params.get(self.page_number_param, 1))

    @cached_property
    def page_size(self) -> int:
        return int(self.request.query_params.get(self.page_size_param, self.default_page_size))

    @cached_property
    def offset(self) -> int:
        return int((self.page - 1) * self.page_size)

    @cached_property
    def count(self) -> int:
        return self.cursor.rowcount

    def _scroll(self):
        self.cursor.scroll(self.offset, mode='absolute')

    def has_next(self) -> bool:
        return self.count > self.offset + self.page_size

    def has_previous(self) -> bool:
        return self.page > 1

    @cached_property
    def url(self):
        return self.request.build_absolute_uri()

    def get_next_link(self) -> Optional[str]:
        if not self.has_next():
            return None
        url = self.url
        page_number = self.page + 1
        return replace_query_param(url, self.page_number_param, page_number)

    def get_previous_link(self) -> Optional[str]:
        if not self.has_previous():
            return None
        url = self.url
        page_number = self.page - 1
        if page_number == 1:
            return remove_query_param(url, self.page_number_param)
        return replace_query_param(url, self.page_number_param, page_number)

    @cached_property
    def data(self) -> List:
        self._scroll()
        columns = sp_loader().columns_from_cursor(self.cursor)
        return [sp_loader().row_to_dict(row, columns) for row in self.cursor.fetchmany(self.page_size)]

    def response(self, serializer: Optional[Callable] = None) -> Response:
        data = self.data
        if serializer is not None:
            serializer = serializer(data=data, many=True, context={'request': self.request})
            serializer.is_valid()
            data = serializer.data
        return Response(
            OrderedDict(
                [
                    ('count', self.count),
                    ('next', self.get_next_link()),
                    ('previous', self.get_previous_link()),
                    ('results', data)
                ]
            )
        )
