import pytest

from malleefowl.tests.common import WpsTestClient, TESTDATA, assert_response_success


@pytest.mark.online
def test_wps_thredds_download():
    wps = WpsTestClient()
    datainputs = "[url={0}]".format(TESTDATA['noaa_catalog_1'])
    resp = wps.get(service='wps', request='execute', version='1.0.0', identifier='thredds_download',
                   datainputs=datainputs)
    assert_response_success(resp)
