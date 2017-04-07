from owslib.wps import WebProcessingService
from owslib.wps import ComplexDataInput

from dispel4py.workflow_graph import WorkflowGraph
from dispel4py import simple_process
from dispel4py.core import GenericPE

from malleefowl.workflow import run as run_basic_workflow

import logging
logger = logging.getLogger("PYWPS")


class MonitorPE(GenericPE):
    def __init__(self):
        GenericPE.__init__(self)

        self._monitor = None
        self._pstart = 0
        self._pend = 100

    def set_monitor(self, monitor, start_progress=0, end_progress=100):
        self._monitor = monitor
        self._pstart = start_progress
        self._pend = end_progress


class GenericWPS(MonitorPE):
    STATUS_NAME = 'status'
    STATUS_LOCATION_NAME = 'status_location'

    def __init__(self, url, identifier, inputs=[], linked_inputs=[], headers=None, **kwargs):
        MonitorPE.__init__(self)

        def log(message, progress):
            if self._monitor:
                self._monitor("{0}: {1}".format(
                    self.identifier, message),
                    progress)
            else:
                logger.info('STATUS ({0}: {2}/100) - {1}'.format(
                    self.identifier, message, progress))

        self.monitor = log

        self._add_output(self.STATUS_NAME)
        self._add_output(self.STATUS_LOCATION_NAME)

        self.identifier = identifier
        self.wps = WebProcessingService(url=url, skip_caps=True, verify=False, headers=headers)
        self.proc_desc = self.wps.describeprocess(identifier)

        # Validate that given inputs exist in the wps
        valid_inputs = [wps_input.identifier for wps_input in self.proc_desc.dataInputs]
        for submitted_input in inputs + linked_inputs:
            if submitted_input[0] not in valid_inputs:
                raise Exception('Invalid workflow : Input "{input}" of process "{proc}" is unknown.'.format(
                    input=submitted_input[0],
                    proc=identifier))

        # These are the static inputs
        # (linked inputs will be appended to wps_inputs just before execution by the _set_inputs function)
        self.wps_inputs = inputs

        # Will be filled as PE are connected to us (by the require_output function)
        self.wps_outputs = []

        self.linked_inputs = []
        for linked_input in linked_inputs:
            # Here we add PE input that will need to be connected
            self._add_input(linked_input[0])

            self.linked_inputs.append({'input': linked_input[0],
                                       'identifier': linked_input[1]['identifier'],
                                       'output': linked_input[1]['output'],
                                       'as_reference': linked_input[1]['as_reference']})

    def progress(self, execution):
        return int(self._pstart +
                   ((self._pend - self._pstart) /
                    100.0 * execution.percentCompleted))

    def monitor_execution(self, execution):
        progress = self.progress(execution)
        self.monitor("status_location={0.statusLocation}".format(execution), progress)

        while execution.isNotComplete():
            try:
                execution.checkStatus(sleepSecs=3)
            except:
                logger.exception("Could not read status xml document.")
            else:
                progress = self.progress(execution)
                self.monitor(execution.statusMessage, progress)

        if execution.isSucceded():
            for output in execution.processOutputs:
                if output.reference is not None:
                    self.monitor(
                        '{0.identifier}={0.reference} ({0.mimeType})'.
                        format(output),
                        progress)
                else:
                    self.monitor(
                        '{0}={1}'.
                        format(output.identifier, ", ".join(output.data)),
                        progress)
        else:
            self.monitor('\n'.join(
                ['ERROR: {0.text} code={0.code} locator={0.locator})'.
                    format(ex) for ex in execution.errors]), progress)

    def extract_result(self, output, as_reference):
        # Downstream process wants a reference...
        if as_reference:
            # and we got a reference!
            if output.reference:
                return output.reference

        # Downstream process wants the data directly
        else:
            # and we have that!
            if output.data:
                return output.data

            # being good we will try to fulfill the data request for json mimetype reference file
            elif output.reference and output.mimeType == "application/json":
                # read json document with list of urls
                import json
                import urllib2
                try:
                    return json.load(urllib2.urlopen(output.reference))
                except Exception:
                    # Don't raise exceptions coming from that.
                    # Simply raise the default exception about not being able to fulfill the request
                    pass

        # No luck, we cannot fulfill the requested result type
        raise Exception("Workflow error : '{proc}' doesn't produce {req_format}"
                        "for the output '{output}' as expected by the downstream process.".
                        format(proc=self.identifier,
                               output=output.identifier,
                               req_format='reference file' if as_reference else 'embedded data' ))

    def execute(self):
        logger.debug("execute with inputs=%s to get outputs=%s", self.wps_inputs, self.wps_outputs)
        execution = self.wps.execute(
            identifier=self.identifier,
            inputs=self.wps_inputs,
            output=self.wps_outputs,
            lineage=True)
        self.monitor_execution(execution)

        result = {self.STATUS_NAME: execution.status,
                  self.STATUS_LOCATION_NAME: execution.statusLocation}

        if execution.isSucceded():
            # NOTE: only set workflow output if specific output was needed
            for wps_output in self.wps_outputs:
                for output in execution.processOutputs:
                    if wps_output[0] == output.identifier:
                        result[wps_output[0]] = self.extract_result(output, wps_output[1])
                        break
            return result
        else:
            failure_msg = '\n'.join(['{0.text}'.
                                    format(ex) for ex in execution.errors])
            raise Exception(failure_msg)

    def require_output(self, output, as_reference):
        # Validate that the required output exists in the wps
        valid_outputs = [wps_output.identifier for wps_output in self.proc_desc.processOutputs]
        if output not in valid_outputs:
            return False

        # Here we add a PE output that is required and about to be connected
        self._add_output(output)

        self.wps_outputs.append((output, as_reference))
        return True

    def _set_inputs(self, inputs):
        wps_inputs_datatype = {wps_input.identifier: wps_input.dataType for wps_input in self.proc_desc.dataInputs}

        for input_name in inputs.keys():
            if input_name in wps_inputs_datatype:
                is_complex = wps_inputs_datatype[input_name] == 'ComplexData'
                for value in inputs[input_name]:
                    if is_complex:
                        value = ComplexDataInput(value)
                    self.wps_inputs.append((input_name, value))

    def process(self, inputs):
        try:
            result = self._process(inputs)
            if result is not None:
                return result
        except Exception:
            logger.exception("process failed!")
            raise

    def _process(self, inputs):
        self._set_inputs(inputs)
        return self.execute()


def run(workflow, monitor=None, headers=None):
    if 'tasks' not in workflow:
        run_basic_workflow(workflow, monitor, headers)

    graph = WorkflowGraph()

    # Create WPS processes and append them in a task array
    wps_tasks = []
    for task in workflow['tasks']:
        progress_range = task.get('progress_range', [0, 100])
        wps_task = GenericWPS(headers=headers, **task)
        wps_task.set_monitor(monitor,
                             progress_range[0],
                             progress_range[1])
        wps_tasks.append(wps_task)

    # Connect each task PE in the dispel graph using the linked inputs information (raise an exception is some connection cannot be done)
    for wps_task in wps_tasks:
        # Try to find the referenced PE for each of the linked inputs
        for linked_input in wps_task.linked_inputs:
            found_linked_input = False

            # Loop in the task array searching for the linked process identifier
            for source_wps_task in wps_tasks:
                if source_wps_task.identifier == linked_input['identifier']:
                    # The linked process has been found
                    # The require_output function will validate the existence of the required output and prepare it
                    # which involve creating the graph PE output and the requested WPS output as reference or not
                    if source_wps_task.require_output(output=linked_input['output'],
                                                      as_reference=linked_input['as_reference']):
                        # The required output has been found we can now safely connect both PE in the graph
                        graph.connect(source_wps_task, linked_input['output'],
                                      wps_task, linked_input['input'])
                        found_linked_input = True
                    break
            # Unfortunately the linked input has not been resolved, we must raise an exception for that
            if not found_linked_input:
                raise Exception(
                    'Cannot build workflow graph : Task "{task}" has an unknown linked input : '
                    'identifier: {identifier}, output: {output}'.format(
                        task=wps_task.identifier,
                        identifier=linked_input['identifier'],
                        output=linked_input['output']))

    # Search for the 'source' PEs (which have no inputs)
    source_PE = {}
    for wps_task in wps_tasks:
        if not wps_task.linked_inputs:
            source_PE[wps_task] = [{}]

    # Run the graph
    result = simple_process.process(graph, inputs = source_PE)

    summary = {}
    for wps_task in wps_tasks:
        summary[wps_task.identifier] =\
            {'status_location': result.get((wps_task.id, wps_task.STATUS_LOCATION_NAME))[0],
             'status': result.get((wps_task.id, wps_task.STATUS_NAME))[0]
             }
    return summary
