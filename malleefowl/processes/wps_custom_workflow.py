import yaml
import traceback
from datetime import datetime

from pywps import Process
from pywps import ComplexInput
from pywps import ComplexOutput
from pywps import Format
from pywps.app.Common import Metadata
from multiprocessing import Manager, Value

from malleefowl.custom_workflow import run
from malleefowl.utils import Monitor


import logging
logger = logging.getLogger("PYWPS")


class DispelCustomWorkflow(Process, Monitor):
    def __init__(self):
        inputs = [
            ComplexInput('workflow', 'Workflow description',
                         abstract='Workflow description in YAML.',
                         metadata=[Metadata('Info')],
                         min_occurs=1,
                         max_occurs=1,
                         supported_formats=[Format('text/yaml')]),
        ]
        outputs = [
            ComplexOutput('output', 'Workflow result',
                          abstract="Workflow result document in YAML.",
                          as_reference=True,
                          supported_formats=[Format('text/yaml')]),
            ComplexOutput('logfile', 'Workflow log file',
                          abstract="Workflow log file.",
                          as_reference=True,
                          supported_formats=[Format('text/plain')]),
        ]

        Process.__init__(
            self,
            self._handler,
            identifier="custom_workflow",
            title="Custom Workflow",
            version="0.1",
            abstract="Runs custom workflow with dispel4py.",
            metadata=[
                Metadata('Birdhouse', 'http://bird-house.github.io/'),
                Metadata('User Guide', 'http://malleefowl.readthedocs.io/en/latest/'),
            ],
            inputs=inputs,
            outputs=outputs,
            status_supported=True,
            store_supported=True,
        )
        synch = Manager()
        self.full_log = synch.list()
        self.overall_progress = synch.dict()
        self.exceptions_list = synch.list()

    def _handler(self, request, response):
        # Reset and preparation
        del self.full_log[:]
        self.overall_progress['progress'] = 0
        self.response = response
        self.update_status("starting workflow ...", 0)

        # Load the workflow
        workflow = yaml.load(request.inputs['workflow'][0].stream)
        workflow_name = workflow.get('name', 'unknown')
        self.update_status("workflow {0} prepared.".format(workflow_name), 0)
        self.full_log.append(yaml.dump(workflow))

        # Prepare headers
        headers = {}
        if 'X-X509-User-Proxy' in request.http_request.headers:
            headers['X-X509-User-Proxy'] = request.http_request.headers['X-X509-User-Proxy']
        if 'Access-Token' in request.http_request.headers:
            headers['Access-Token'] = request.http_request.headers['Access-Token']

        # Run the workflow
        try:
            result = run(workflow, monitor=self, headers=headers)
            self.update_status("workflow {0} done.".format(workflow_name), 100)
            self.full_log.append('\nWorkflow result:\n{0}'.format(yaml.dump(result)))
        except Exception as e:
            self.raise_exception(e)

        # Handle exceptions (if any)
        if len(self.exceptions_list) > 0:
            full_msg = ('Catch {nb_e} exception(s) while running the workflow:\n'
                        '{exceptions}\n\n'
                        'Execution log:\n{log}').format(
                nb_e=len(self.exceptions_list),
                exceptions='\n'.join(self.exceptions_list),
                log='\n'.join(self.full_log))

            # Augment the exception message by appending the full log but conserve the full exception stack
            raise Exception(full_msg)

        # Send result
        with open('logfile.txt', 'w') as fp:
            fp.write('\n'.join(self.full_log))
            response.outputs['logfile'].file = fp.name

        with open('output.txt', 'w') as fp:
            yaml.dump(result, stream=fp)
            response.outputs['output'].file = fp.name

        return response

    def update_status(self, message, progress=None):
        if not progress:
            progress = self.overall_progress['progress']
        else:
            self.overall_progress['progress'] = progress

        logger.debug('{progress:>4}%: {msg}'.format(progress=progress, msg=message))

        log = '{timestamp}{progress:>4}%: {msg}\n'.format(
            timestamp=datetime.now().strftime('%H:%M:%S'),
            progress=progress,
            msg=message)

        self.response.update_status(message, progress)
        self.full_log.append(log)

    def raise_exception(self, exception):
        self.exceptions_list.append(traceback.format_exc())
