try:
    import numpy as np
except ImportError:
    np = None

from tests.numpy.util import check_numpy
from tests.testcase import BaseTestCase


class NumpyBaseTestCase(BaseTestCase):
    client_kwargs = {'settings': {'use_numpy': True}}

    @check_numpy
    def setUp(self):
        super(NumpyBaseTestCase, self).setUp()

    def assertArraysEqual(self, first, second):
        return self.assertTrue((first == second).all())
