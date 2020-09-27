from tests.testcase import BaseTestCase


class NumpyBaseTestCase(BaseTestCase):
    client_kwargs = {'settings': {'use_numpy': True}}

    def setUp(self):
        try:
            super(NumpyBaseTestCase, self).setUp()
        except RuntimeError as e:
            if 'NumPy' in str(e):
                self.skipTest('Numpy package is not installed')

    def assertArraysEqual(self, first, second):
        return self.assertTrue((first == second).all())
