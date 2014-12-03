import nose.tools
from unittest import TestCase
from nose import SkipTest
from nose.plugins.attrib import attr

from owslib.wps import monitorExecution

from __init__ import TESTDATA, SERVICE

class WpsTestCase(TestCase):
    """
    Base TestCase class, sets up a wps
    """

    @classmethod
    def setUpClass(cls):
        from owslib.wps import WebProcessingService
        cls.wps = WebProcessingService(SERVICE, verbose=False, skip_caps=False)

class WgetTestCase(WpsTestCase):

    @attr('online')
    def test_wget_http(self):
        inputs = []
        inputs.append((
            'resource',
            'http://localhost:8090/wpscache/tasmax_WAS-44_MPI-M-MPI-ESM-LR_historical_r1i1p1_MPI-CSC-REMO2009_v1_day_20010101-20051231.nc'))
        execution = self.wps.execute(identifier="wget", inputs=inputs, output=[('output', True)])
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)
    
    @attr('online')
    def test_wget_file(self):
        # TODO: wget should also accept file urls ...
        raise SkipTest
        inputs = []
        inputs.append((
            'resource',
            TESTDATA['tasmax_WAS-44_MPI-M-MPI-ESM-LR_historical_r1i1p1_MPI-CSC-REMO2009_v1_day_20010101-20051231.nc']))
        execution = self.wps.execute(identifier="wget", inputs=inputs, output=[('output', True)])
        monitorExecution(execution, sleepSecs=1)

        nose.tools.ok_(execution.status == 'ProcessSucceeded', execution.status)



