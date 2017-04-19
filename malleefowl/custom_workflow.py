import json
import urllib2

from owslib.wps import WebProcessingService
from owslib.wps import ComplexDataInput
from owslib.wps import printInputOutput

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

    def __init__(self, name, url, identifier, inputs=[], linked_inputs=[], headers=None, **kwargs):
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

        self.name = name
        self.identifier = identifier
        self.wps = WebProcessingService(url=url, skip_caps=True, verify=False, headers=headers)
        self.proc_desc = self.wps.describeprocess(identifier)

        # Validate that given inputs exist in the wps
        valid_inputs = [wps_input.identifier for wps_input in self.proc_desc.dataInputs]
        # Allow a process not requiring any input to be linked to a previous one by using a "None" input name
        valid_inputs.append("None")
        for submitted_input in inputs + linked_inputs:
            if submitted_input[0] not in valid_inputs:
                raise Exception('Invalid workflow : Input "{input}" of process "{proc}" is unknown.'.format(
                    input=submitted_input[0],
                    proc=identifier))

        # These are the static inputs
        # (linked inputs will be appended to wps_inputs just before execution by the _set_inputs function)
        self.wps_inputs = [(input[0], input[1]) for input in inputs]

        # Will be filled as PE are connected to us (by the require_output function)
        self.wps_outputs = []

        self.linked_inputs = {}
        for linked_input in linked_inputs:
            # Here we add PE input that will need to be connected
            self._add_input(linked_input[0])
            self.linked_inputs[linked_input[0]] = linked_input[1]

    def require_output(self, output, as_reference):
        # Validate that the required output exists in the wps
        valid_outputs = [wps_output.identifier for wps_output in self.proc_desc.processOutputs]
        if output not in valid_outputs:
            return False

        # Here we add a PE output that is required and about to be connected
        self._add_output(output)

        self.wps_outputs.append((output, as_reference))
        return True

    def process(self, inputs):
        try:
            self._set_inputs(inputs)
            result = self._execute()
            if result is not None:
                return result
        except Exception:
            logger.exception("process failed!")
            raise

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

    def _get_exception(self, wps_output, wps_input, as_ref):
        # No luck, we cannot fulfill the requested result type
        details = "Upstream task '{out}' output doesn't produce a compatible format for '{input}' input of '{task}'.".\
            format(out=wps_output.identifier,
                   input=wps_input.identifier,
                   task=self.name)
        more = 'Output :\n{out}\nInput (is reference : {as_ref}) :\n{input}'.format(
            out=printInputOutput(wps_output),
            input=printInputOutput(wps_input),
            as_ref=as_ref)
        msg = 'Workflow datatype incompatibility error : {details}\n{more}'.format(details=details, more=more)
        return Exception(msg)

    def _read_reference(self, reference):
        # read the reference content
        try:
            return urllib2.urlopen(reference).read()
        except Exception:
            # Don't raise exceptions coming from that.
            return None

    def _adapt(self, wps_output, wps_input):
        as_reference = self.linked_inputs[wps_input.identifier]['as_reference']

        # Downstream process wants a reference, so consider the reference as the data from this point
        if as_reference:
            wps_output.data = wps_output.reference

        # Downstream process wants the data directly, but we only have the reference: Extract the data!
        elif not wps_output.data and wps_output.reference:
            wps_output.data = self._read_reference(wps_output.reference)

        # process output data are append into a list so extract the first value here
        elif isinstance(wps_output.data, list) and len(wps_output.data) == 1:
            wps_output.data = wps_output.data[0]

        # Is it possible to have more than one output?
        else:
            raise self._get_exception(wps_output, wps_input, as_reference)


        # At this point raise an exception if we don't have data in wps_output.data
        if not wps_output.data:
            raise self._get_exception(wps_output, wps_input, as_reference)

        # Consider the validation completed if the dataType match for non-complex data or
        # if the mimetype match for complex data
        is_complex = wps_input.dataType == 'ComplexData'
        supported_mimetypes = [value.mimeType for value in wps_input.supportedValues] if is_complex else []
        if wps_output.dataType == wps_input.dataType and (not is_complex or wps_output.mimeType in supported_mimetypes):
            # Covered cases:
            # _ string -> string
            # _ ref string -> ref string
            # _ ref string -> string
            # _ integer -> integer
            # _ ref integer -> ref integer
            # _ ref integer -> integer
            # _ bbox -> bbox
            # _ ref bbox -> ref bbox
            # _ ref bbox -> bbox
            # X complexdata -> complexdata
            # _ ref complexdata -> ref complexdata
            # _ ref complexdata -> complexdata
            return [wps_output.data, ]

        # Remain cases where we have mismatch for datatypes or complex data mimetypes...
        # Before raising an exception we will check for a specific case that we will handle:
        # json array that could be fed into the downstream wps wanting an array of data too.
        # If this specific case is detected we will simply send the json content to the downstream wps without further
        # validation since the json content type cannot be verified.
        take_array = wps_input.maxOccurs > 1
        if take_array and wps_output.mimeType == 'application/json':
            # If the json data was still referenced read it now
            if as_reference:
                wps_output.data = self._read_reference(wps_output.reference)

            json_data = json.loads(wps_output.data)
            if isinstance(json_data, list):
                array = []
                for value in json_data:
                    array.append(ComplexDataInput(value) if is_complex else value)

                return array

        raise self._get_exception(wps_output, wps_input, as_reference)

    def _set_inputs(self, inputs):
        wps_inputs = {wps_input.identifier: wps_input for wps_input in self.proc_desc.dataInputs}

        for key in inputs.keys():
            if key in wps_inputs:
                # This is the upstream wps output object
                wps_output = inputs[key]

                # This is the current wps input object
                wps_input = wps_inputs[key]

                # Now we try to do most of the conversion job between these two datatypes with the knowledge we have
                # and append the new wps input into the list
                self.wps_inputs += [(key, value) for value in self._adapt(wps_output, wps_input)]

    def _execute(self):
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
                        # Send directly the wps output object to the downstream PE
                        # Note: output.data is always an array since a wps output is append to processOutputs[x].data
                        result[wps_output[0]] = output
                        break
            return result
        else:
            failure_msg = '\n'.join(['{0.text}'.
                                    format(ex) for ex in execution.errors])
            raise Exception(failure_msg)


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
        for input_name, linked_input in wps_task.linked_inputs.iteritems():
            found_linked_input = False

            # Loop in the task array searching for the linked task
            for source_wps_task in wps_tasks:
                if source_wps_task.name == linked_input['task']:
                    # The linked process has been found
                    # The require_output function will validate the existence of the required output and prepare it
                    # which involve creating the graph PE output and the requested WPS output as reference or not
                    if source_wps_task.require_output(output=linked_input['output'],
                                                      as_reference=linked_input['as_reference']):
                        # The required output has been found we can now safely connect both PE in the graph
                        graph.connect(source_wps_task, linked_input['output'],
                                      wps_task, input_name)
                        found_linked_input = True
                    break
            # Unfortunately the linked input has not been resolved, we must raise an exception for that
            if not found_linked_input:
                raise Exception(
                    'Cannot build workflow graph : Task "{task}" has an unknown linked input : '
                    'task: {linked_task}, output: {output}'.format(
                        task=wps_task.name,
                        linked_task=linked_input['task'],
                        output=linked_input['output']))

    # Search for the 'source' PEs (which have no inputs)
    source_PE = {}
    for wps_task in wps_tasks:
        if not wps_task.linked_inputs:
            source_PE[wps_task] = [{}]

    # Run the graph
    try:
        result = simple_process.process(graph, inputs = source_PE)
    except Exception as e:
        raise Exception(
            'Cannot run the workflow graph : {0}'.format(e.message))

    summary = {}
    for wps_task in wps_tasks:
        summary[wps_task.name] =\
            {'status_location': result.get((wps_task.id, wps_task.STATUS_LOCATION_NAME))[0],
             'status': result.get((wps_task.id, wps_task.STATUS_NAME))[0]
             }
    return summary
