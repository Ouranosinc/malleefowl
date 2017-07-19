import json
import traceback
from datetime import datetime

from pywps import Process
from pywps import ComplexInput
from pywps import ComplexOutput
from pywps import Format
from pywps.app.Common import Metadata
from multiprocessing import Manager

from malleefowl.custom_workflow import run
from malleefowl.utils import Monitor
from malleefowl.exceptions import WorkflowException

import logging
logger = logging.getLogger("PYWPS")


class PyWPSMonitor(Monitor):
    """
    Implement the task monitoring class malleefowl.utils.Monitor for the PyWPS needs
    """
    def __init__(self, response):
        synch = Manager()
        self.full_log = synch.list()
        self.exceptions_list = synch.list()
        self.result_summary = synch.dict()
        self.overall_progress = synch.dict()
        self.overall_progress['progress'] = 0
        self.response = response

    def update_status(self, message, progress=None):
        """
        Implement malleefowl.utils.Monitor.update_status function. See Monitor.update_status for details
        """
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
        """
        Implement malleefowl.utils.Monitor.raise_exception function. See Monitor.raise_exception for details
        """
        self.exceptions_list.append(traceback.format_exc())

    def save_task_result(self, task, result):
        """
        Implement malleefowl.utils.Monitor.save_task_result function. See Monitor.save_task_result for details
        """
        if task in self.result_summary:
            task_result = self.result_summary[task]
            task_result['processes'].append(result)
            self.result_summary.update({task: task_result})
        else:
            result.update()
            self.result_summary.update({task: dict(execution_order=len(self.result_summary) + 1,
                                                   processes=[result, ])})

    def format_summary(self):
        """
        Format the summary for a better looking
        """
        ordered_task = sorted(self.result_summary.items(), key=lambda x: x[1]['execution_order'])

        return [{task[0]: sorted(task[1]['processes'], key=lambda x: x.get('data_id', 0))} for task in ordered_task]


class DispelCustomWorkflow(Process):
    """
    Implement a PyWPS process for executing custom workflow
    """
    def __init__(self):
        inputs = [
            ComplexInput('workflow', 'Workflow description',
                         abstract='Workflow description in JSON.',
                         metadata=[Metadata('Info')],
                         min_occurs=1,
                         max_occurs=1,
                         supported_formats=[Format('application/json')]),
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

    def _handler(self, request, response):
        """
        Implement the PyWPS process handler
        :param request:
        :param response:
        :return:
        """

        monitor = PyWPSMonitor(response)
        monitor.update_status("starting workflow ...", 0)

        # Load the workflow
        workflow = json.load(request.inputs['workflow'][0].stream)
        workflow_name = workflow.get('name', 'unknown')
        monitor.update_status("workflow {0} prepared:".format(workflow_name), 0)
        monitor.full_log.append(json.dumps(workflow,
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
            run(workflow, monitor=monitor, headers=headers)
            monitor.update_status("workflow {0} done.".format(workflow_name), 100)

            formatted_summary = monitor.format_summary()
            monitor.full_log.append('Workflow result:')
            monitor.full_log.append(json.dumps(formatted_summary,
                                               indent=4,
                                               separators=(',', ': '),
                                               sort_keys=True))
        except Exception as e:
            formatted_summary = None
            monitor.raise_exception(e)

        # Handle exceptions (if any)
        if len(monitor.exceptions_list) > 0:
            full_msg = ('\nCatch {nb_e} exception(s) while running the workflow:\n'
                        '{exceptions}\n\n'
                        'Execution log:\n{log}').format(
                nb_e=len(monitor.exceptions_list),
                exceptions='\n'.join(monitor.exceptions_list),
                log='\n'.join(monitor.full_log))

            # Augment the exception message by appending the full log but conserve the full exception stack
            raise WorkflowException(full_msg)

        # Send result
        with open('logfile.txt', 'w') as fp:
            fp.write('\n'.join(monitor.full_log))
            response.outputs['logfile'].file = fp.name

        with open('output.json', 'w') as fp:
            fp.write(json.dumps(formatted_summary, sort_keys=True))
            response.outputs['output'].file = fp.name

        return response
