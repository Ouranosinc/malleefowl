import nose.tools
from nose import SkipTest

import os
import tempfile

from malleefowl import restflow

service="http://localhost:8090/wps"

# set path to buildout/bin to have access to restflow binary
os.environ['PATH'] = '%s:%s' % (
    os.path.join(os.path.dirname(restflow.__file__), '..', '..', '..', 'bin'),
    os.environ['PATH'])


def test_generate():
    wf = restflow.generate("zeroWorkflow",
                           service=service,
                           identifier="org.malleefowl.test.dummyprocess",
                           input=['input1=1', 'input2=2'])
    nose.tools.ok_("WpsExecute" in wf, wf)
    nose.tools.ok_(service in wf, wf)
    nose.tools.ok_('input2' in wf, wf)

def test_run():
    wf = restflow.generate("zeroWorkflow",
                           service=service,
                           identifier="org.malleefowl.test.dummyprocess",
                           input=['input1=1', 'input2=2'])

    (fp, filename) = tempfile.mkstemp(suffix=".yaml", prefix="restflow-")
    restflow.write(filename, wf)

    result = restflow.run(filename, basedir=tempfile.mkdtemp(), verbose=True)

    nose.tools.ok_(False, result)
