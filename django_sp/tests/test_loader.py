from django_sp.tests.base import BaseTestCase


class LoaderTestCase(BaseTestCase):
    def setUp(self):
        super(LoaderTestCase, self).setUp()
        cursor = self.sp_loader.connection.cursor()
        cursor.execute("INSERT INTO test_table (name, amount) VALUES ('test', 100)")
        cursor.execute("INSERT INTO test_table (name, amount) VALUES ('test2', 200)")
        cursor.close()

    def test_loaded(self):
        self.assertEqual(len(self.sp_loader), 2)

    def test_procedure(self):
        self.assertTrue('test_function' in self.sp_loader)
        self.assertEqual(self.sp_loader.test_function(100), (400,))

    def test_view(self):
        self.assertTrue('test_view' in self.sp_loader)
        # Fetch all
        self.assertEqual(
            self.sp_loader.test_view(ret='all'),
            [
                {'id': 1, 'name': 'test', 'amount': 200},
                {'id': 2, 'name': 'test2', 'amount': 400}
            ]
        )

        # Fetch one
        self.assertEqual(
            self.sp_loader.test_view(), {'id': 1, 'name': 'test', 'amount': 200}
        )

        # Fetch all with filters
        self.assertEqual(
            self.sp_loader.test_view(filters='amount > %s', params=(300,), ret='all'),
            [
                {'id': 2, 'name': 'test2', 'amount': 400}
            ]
        )

        # Fetch with cursor
        cursor = self.sp_loader.test_view(filters="   ", ret='cursor')
        columns = self.sp_loader.columns_from_cursor(cursor)

        self.assertEqual(self.sp_loader.row_to_dict(cursor.fetchone(), columns),
                         {'id': 1, 'name': 'test', 'amount': 200})
        self.assertEqual(self.sp_loader.row_to_dict(cursor.fetchone(), columns),
                         {'id': 2, 'name': 'test2', 'amount': 400})
        self.assertEqual(self.sp_loader.row_to_dict(cursor.fetchone(), columns), None)
