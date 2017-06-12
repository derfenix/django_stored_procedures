from decimal import Decimal

from django.core.exceptions import ValidationError

from django_sp.helpers.rest_framework import DecimalFilter, IntegerFilter, RawSQLFilterSet, StringFilter
from django_sp.tests.base import BaseTestCase


class GenericFilterSet(RawSQLFilterSet):
    some_name = StringFilter(map_to='name', default='noop')
    age = IntegerFilter(max_value=100, min_value=10)
    amount = DecimalFilter(max_value=400, min_value=-300)


    class Meta:
        order_by = '-amount'


class GenericORFilterSet(RawSQLFilterSet):
    some_name = StringFilter(map_to='name', default='noop')
    age = IntegerFilter(max_value=100, min_value=10)
    amount = DecimalFilter(max_value=400, min_value=-300)


    class Meta:
        order_by = 'amount'
        logical_or = ('age', 'amount')


class Request:
    query_params = None

    def __init__(self, query_params):
        self.query_params = query_params


class DRFHelperTestCase(BaseTestCase):
    def test_exceptions(self):
        request = Request(
            {'some_name': 'ololo', 'age': 150, 'amount': '100.5'}
        )

        filterset = GenericFilterSet(request)
        with self.assertRaises(AssertionError):
            _ = filterset.params

        with self.assertRaisesMessage(ValidationError,
                                      '["Exception raised for age: [\'Value can not be greater than 100\']"]'):
            _ = filterset.sql

    def test_fields_ok(self):
        request = Request(
            {'some_name': 'ololo', 'age': 50, 'amount': '100.5'}
        )

        filterset = GenericFilterSet(request)

        self.assertEqual(filterset.sql.strip(), 'name = %s AND age = %s AND amount = %s ORDER BY amount DESC')
        self.assertEqual(filterset.params, ('ololo', 50, Decimal('100.5')))

        request = Request(
            {'some_name': 'ololo', 'age__gte': 50, 'amount': '100.5'}
        )
        filterset = GenericFilterSet(request)
        self.assertEqual(filterset.sql.strip(), 'name = %s AND age >= %s AND amount = %s ORDER BY amount DESC')
        self.assertEqual(filterset.params, ('ololo', 50, Decimal('100.5')))

        request = Request(
            {'some_name': 'ololo', 'age__isnull': 'false'}
        )
        filterset = GenericFilterSet(request)
        self.assertEqual(filterset.sql.strip(), 'name = %s AND age IS NOT NULL ORDER BY amount DESC')
        self.assertEqual(filterset.params, ('ololo',))

    def test_logical_or(self):
        request = Request(
            {'some_name': 'ololo', 'age': 50, 'amount': '100.5'}
        )

        filterset = GenericORFilterSet(request)

        self.assertEqual(filterset.sql.strip(), 'name = %s AND (age = %s OR amount = %s) ORDER BY amount ASC')
        self.assertEqual(filterset.params, ('ololo', 50, Decimal('100.5')))

        request = Request(
            {'some_name': 'ololo'}
        )

        filterset = GenericORFilterSet(request)
        self.assertEqual(filterset.sql.strip(), 'name = %s AND (TRUE) ORDER BY amount ASC')
