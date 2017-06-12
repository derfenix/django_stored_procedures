from django.test import TestCase


class BaseTestCase(TestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.sp_loader.populate_helper()
        self.sp_loader.load_sp_into_db()

    @property
    def sp_loader(self):
        from django_sp import sp_loader

        return sp_loader
