import os
import tempfile

from mako.template import Template
from mako.lookup import TemplateLookup

mylookup = TemplateLookup(directories=[os.path.join(os.path.dirname(__file__), 'templates')],
                          module_directory=os.path.join(tempfile.gettempdir(), 'mako_cache'))

import logging
logger = logging.getLogger(__name__)

def generate():
    mytemplate = mylookup.get_template('simpleWorkflow.yaml')
    return  mytemplate.render()

def write(filename, workflow):
    with open(filename, 'w') as fp:
        fp.write(workflow)

def run(filename, verbose=False):
    import subprocess
    from subprocess import PIPE

    logger.debug("filename = %s", filename)
    
    cmd = ["restflow", "-f", filename, "--run", "restflow"]
    if verbose:
        cmd.append('-t')
    p = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE)

    (stdoutdata, stderrdata) = p.communicate()
    
