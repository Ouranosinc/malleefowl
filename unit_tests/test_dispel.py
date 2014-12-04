from malleefowl.dispel import esgsearch_workflow

from nose import tools
from unittest import TestCase
from nose import SkipTest
from nose.plugins.attrib import attr

from __init__ import SERVICE, TESTDATA, CREDENTIALS

@attr('online')
@attr('security')
def test_esgsearch_workflow():
    # TODO: set environ with credentials
    constraints = 'project:CORDEX,experiment:historical,variable:tas,time_frequency:mon'
    result = esgsearch_workflow(SERVICE, CREDENTIALS, constraints)
    tools.ok_( len(result) == 1, result)
    tools.ok_('hummingbird' in result.values()[0][0][0], result.values()[0])
    #tools.eq_({(prev.id, 'output'): [1, 2, 3, 4, 5]}, result)
    #tools.ok_(False, result)
    
    
    




