import json
import traceback
from datetime import datetime

from pywps import Process
from pywps import LiteralInput
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
                         abstract='Workflow description in JSON.',
                         metadata=[Metadata('Info')],
                         min_occurs=0,
                         max_occurs=1,
                         supported_formats=[Format('application/json')]),
            LiteralInput('workflow_string', 'Workflow description',
                         data_type='string',
                         abstract="Workflow description in json as string",
                         min_occurs=0,
                         max_occurs=1)
        ]
        outputs = [
            ComplexOutput('output', 'Workflow result',
                          abstract="Workflow result document in JSON.",
                          as_reference=False,
                          supported_formats=[Format('application/json')]),
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
        self.result_summary = synch.dict()

    def _handler(self, request, response):
        # Reset and preparation
        del self.full_log[:]
        del self.exceptions_list[:]
        self.result_summary.clear()
        self.overall_progress['progress'] = 0
        self.response = response
        self.update_status("starting workflow ...", 0)

        # Load the workflow
        if 'workflow' in request.inputs:
            workflow = json.load(request.inputs['workflow'][0].stream)
        else:
            workflow = json.loads(request.inputs['workflow_string'][0].data)

        workflow_name = workflow.get('name', 'unknown')
        self.update_status("workflow {0} prepared:".format(workflow_name), 0)
        self.full_log.append(json.dumps(workflow,
                                        indent=4,
                                        separators=(',', ': ')))

        # Prepare headers
        headers = {}
        if 'X-X509-User-Proxy' in request.http_request.headers:
            headers['X-X509-User-Proxy'] = request.http_request.headers['X-X509-User-Proxy']
        if 'Access-Token' in request.http_request.headers:
            headers['Access-Token'] = request.http_request.headers['Access-Token']

        # Run the workflow
        try:
            run(workflow, monitor=self, headers=headers)
            self.update_status("workflow {0} done.".format(workflow_name), 100)

            formatted_summary = self._format_summary()
            self.full_log.append('Workflow result:')
            self.full_log.append(json.dumps(formatted_summary,
                                            indent=4,
                                            separators=(',', ': '),
                                            sort_keys=True))
        except Exception as e:
            self.raise_exception(e)

        # Handle exceptions (if any)
        if len(self.exceptions_list) > 0:
            full_msg = ('\nCatch {nb_e} exception(s) while running the workflow:\n'
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

        with open('output.json', 'w') as fp:
            fp.write(json.dumps(formatted_summary, sort_keys=True))
            response.outputs['output'].file = fp.name

        return response

    def update_status(self, message, progress=None):
        if not progress:
            progress = self.overall_progress['progress']
        else:
            self.overall_progress['progress'] = progress

        logger.debug('{progress:>4}%: {msg}'.format(progress=progress, msg=message))

        log = '{timestamp}{progress:>4}%: {msg}'.format(
            timestamp=datetime.now().strftime('%H:%M:%S'),
            progress=progress,
            msg=message)

        self.response.update_status(message, progress)
        self.full_log.append(log)

    def raise_exception(self, exception):
        self.exceptions_list.append(traceback.format_exc())

    def save_task_result(self, task, result):
        if task in self.result_summary:
            task_result = self.result_summary[task]
            task_result['processes'].append(result)
            self.result_summary.update({task: task_result})
        else:
            result.update()
            self.result_summary.update({task: dict(execution_order=len(self.result_summary) + 1,
                                                   processes=[result, ])})

    def _format_summary(self):
        ordered_task = sorted(self.result_summary.items(), key=lambda x: x[1]['execution_order'])

        return [{task[0]: sorted(task[1]['processes'], key=lambda x: x.get('data_id', 0))} for task in ordered_task]
        return ordered_summary
