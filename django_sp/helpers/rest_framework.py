import datetime
import re
from collections import OrderedDict, defaultdict
from decimal import Decimal
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union

from django.core.exceptions import ValidationError
from django.utils.dateparse import parse_datetime
from django.utils.functional import cached_property

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

    def __init__(self, map_to: Optional[str] = None, default: Optional[Any] = None,
                 converter: Optional[Callable] = None):
        self.default = default
        self._map_to = map_to
        if converter is not None:
            self._converter = converter

    @staticmethod
    def _isnull_condition_replace(value: str) -> [str, NoValue]:
        value = value.lower()
        if value == 'true':
            return 'IS NULL', novalue
        elif value == 'false':
            return 'IS NOT NULL', novalue
        else:
            raise ValueError('%s is not right value for `isnull` condition', value)

    def filter(self, name: str, condition: str, value: str) -> Tuple[str, Any]:
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
        return sql, self._parse_value(value)

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

    def __init__(self, input_formats: Optional[List] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_formats = input_formats

    def _convert(self, value: Any):
        return parse_datetime(value)


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
                        sql, value = filter_.filter(name, condition, value)
                    except ValidationError as e:
                        raise ValidationError('Exception raised for {}: {}'.format(name, e))
                    self.params = value
                    yield sql
            elif filter_.default is not None:
                self.params = filter_.default
                yield "{} = %s".format(name)
