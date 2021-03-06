import string
import random
from time import sleep

from owslib.wps import WebProcessingService
# For check_status function
from owslib.wps import WPSExecuteReader
from owslib.etree import etree

from malleefowl.pe.progress_monitor import ProgressMonitorPE, RangeProgress, RangeGroupProgress
from malleefowl.utils import DataWrapper
from malleefowl.exceptions import WorkflowException

import logging
logger = logging.getLogger("PYWPS")

# If the xml document is unavailable after 5 attempts consider that the process has failed
XML_DOC_READING_MAX_ATTEMPT = 5


class GenericWPS(ProgressMonitorPE):
    """
    Wrap the execution of a WPS process into a dispel4py PE
    """
    STATUS_NAME = 'status'
    STATUS_LOCATION_NAME = 'status_location'
    OUTPUT_NAME = 'outputs'

    DUMMY_INPUT_NAME = 'None'

    def __init__(self, name, url, identifier,
                 inputs=None,
                 linked_inputs=None,
                 monitor=None,
                 progress_range=None,
                 headers=None,
                 **_):
        """
        :param name: task name
        :param url: url of the WPS provider
        :param identifier: identifier of the WPS task
        :param inputs: dict of static inputs for the wps
                       (multiple values can be passed for each key by using an array)
        :param linked_inputs: dict of dynamic inputs of the wps obtained from upstream tasks
                              (multiple values can be passed for each key by using an array)
        :param monitor: status and progress monitor
        :param progress_provider:
        :param progress_range: progress range of this task in the overall workflow
        :param headers: headers to be included in the wps call
        :param _: Accept any others parameters coming from the workflow description
        """
        ProgressMonitorPE.__init__(self, name, monitor)

        # Convert dict of values or array of values to a flat list of key, value tuple
        inputs = list(self.iter_inputs(inputs))
        linked_inputs = list(self.iter_inputs(linked_inputs))

        # Defaults argument (with mutable type)
        if not progress_range:
            progress_range = [0, 100]

        self.set_progress_provider(RangeProgress(progress_range[0], progress_range[1]))

        self.wps = WebProcessingService(url=url, skip_caps=True, verify=False, headers=headers)
        self.identifier = identifier
        self.proc_desc = self.wps.describeprocess(identifier)

        self._validate_inputs(inputs, linked_inputs)

        # These are the static inputs
        # (linked inputs will be appended to inputs just before execution by the _set_inputs function)
        self.static_inputs = inputs
        self.dynamic_inputs = []

        # Will be filled as PE are connected to us (by the get_output function)
        self.outputs = []

        for _input in linked_inputs:
            # Here we add PE input that will need to be connected
            if _input[0] not in self.inputconnections:
                self._add_input(_input[0])
            self._add_linked_input(_input[0], _input[1])

    def try_connect(self, graph, linked_input, downstream_task, downstream_task_input):
        """
        Override TaskPE fct. See TaskPE.try_connect for details.
        Add the WPS outputs upon connection
        """
        if ProgressMonitorPE.try_connect(self, graph, linked_input, downstream_task, downstream_task_input):
            # Here we added the WPS output name and its as_reference request
            self.outputs.append((linked_input.get('output', self._get_default_output()),
                                 linked_input.get('as_reference', False)))
            return True
        return False

    def _process(self, inputs):
        """
        Implement the GenericPE _process function.
        This function is called multiple time if more than one input must be set
        :param inputs: What has been outputted by the previous task
        :return: None because the WPS execute function will only be called in _postprocess fct
        """

        # Assign the input internally and wait for all inputs before launching the wps execution
        for key, value in self._read_inputs(inputs):
            self.dynamic_inputs.append((key, value))

    def _postprocess(self):
        """
        Implement the GenericPE _postprocess function.
        Call when this PE has received all its inputs and thus are ready to execute the wps
        """

        try:
            self._check_inputs()
            self._send_outputs(self._execute())
        except Exception:
            logger.exception("process failed!")
            raise

    def _connect(self, from_connection, to_node, to_connection, graph):
        """
        Override TaskPE fct. See TaskPE._connect for details.
        We add the PE output just before completing the connection in the graph
        """

        # Here we add a PE output that is required before completing the actual connection
        self._add_output(from_connection)
        ProgressMonitorPE._connect(self, from_connection, to_node, to_connection, graph)

    def _get_default_output(self):
        """
        Implement TaskPE fct.
        If the WPS has only one output it can be set as the default one. Else it has to be set.
        """
        if len(self.proc_desc.processOutputs) == 1:
            return self.proc_desc.processOutputs[0].identifier
        return None

    def get_input_desc(self, input_name):
        """
        Implement TaskPE fct. See TaskPE.get_input_desc for details.
        """
        for wps_input in self.proc_desc.dataInputs:
            if wps_input.identifier == input_name:
                return wps_input
        return None

    def get_output_desc(self, output_name):
        """
        Implement TaskPE fct. See TaskPE.get_output_desc for details.
        """
        for wps_output in self.proc_desc.processOutputs:
            if wps_output.identifier == output_name:
                return wps_output
        return None

    def _validate_inputs(self, inputs, linked_inputs):
        """
        Check that given inputs and linked inputs are valid
        :param inputs: static input (value is available now)
        :param linked_inputs: dynamic inputs (value will be obtained from upstream task)
        """

        # Validate that given inputs exist in the wps
        valid_inputs = [wps_input.identifier for wps_input in self.proc_desc.dataInputs]

        # Allow a process not requiring any input to be linked to a previous one by using a dummy ("None") input name
        valid_inputs.append(self.DUMMY_INPUT_NAME)

        for submitted_input in inputs + linked_inputs:
            if submitted_input[0] not in valid_inputs:
                raise WorkflowException("Invalid workflow : Input '{input}' of process '{proc}' is unknown.".format(
                                        input=submitted_input[0],
                                        proc=self.identifier))

    def _check_inputs(self):
        """
        Check if all the required inputs have been set (from workflow static inputs or from upstream tasks)
        """
        ready_inputs = [_input[0] for _input in self.static_inputs + self.dynamic_inputs]

        # Consider the dummy input to be always ready!
        ready_inputs.append(self.DUMMY_INPUT_NAME)

        for linked_input in self.linked_inputs:
            if linked_input[0] not in ready_inputs:
                msg = "Workflow cannot complete because of a missing input '{input}' of task {task}".format(
                    input=linked_input[0],
                    task=self.name)
                raise WorkflowException(msg)

    @staticmethod
    def _check_status(execution):
        """
        Try to read the xml status of the underlying WPS process, raise Exception if the url cannot be read properly
        """
        reader = WPSExecuteReader(verbose=execution.verbose)
        # override status location
        logger.info('Checking execution status : %s' % execution.statusLocation)
        try:
            response = reader.readFromUrl(
                execution.statusLocation,
                username=execution.username,
                password=execution.password,
                verify=execution.verify,
                headers=execution.headers)
            response = etree.tostring(response)
        except Exception:
            raise
        else:
            execution.checkStatus(response=response, sleepSecs=3)

    def _monitor_execution(self, execution):
        """
        Monitor the execution of the underlying WPS and return only when the process end (successfully or not)
        """

        progress = self.progress(execution)
        self.monitor("status_location={0.statusLocation}".format(execution), progress)

        xml_doc_read_failure = 0

        last_status_message = None
        last_progress = None
        while execution.isNotComplete():
            try:
                # Check the status of the wps execution
                self._check_status(execution)
            except Exception as e:
                # Try XML_DOC_READING_MAX_ATTEMPT time before raising an exception
                xml_doc_read_failure += 1
                if xml_doc_read_failure > XML_DOC_READING_MAX_ATTEMPT:
                    logger.error("Failed to read status xml document after %s attempts : %s",
                                 XML_DOC_READING_MAX_ATTEMPT,
                                 str(e))
                    raise
                else:
                    # Sleep 5 seconds to give a chance
                    sleep(5)
            else:
                progress = self.progress(execution)
                if execution.statusMessage != last_status_message or progress != last_progress:
                    last_status_message = execution.statusMessage
                    last_progress = progress
                    self.monitor(execution.statusMessage, progress)

        self.monitor(execution.statusMessage, progress)

        # In case of success log all output value
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

        # Or log the errors
        else:
            self.monitor('\n'.join(
                ['ERROR: {0.text} code={0.code} locator={0.locator})'.
                    format(ex) for ex in execution.errors]), progress)

    def get_output_datatype(self, output):
        """
        Extract the correct datatype from the datatype structure
        :param output: output as parsed from the wps xml status (ows.wps.Output)
        :return: The datatype of the output as a string (ComplexData, string, etc.)
        """
        # outputs from execution structure do not always carry the dataType
        # so find it from the process description
        if not output.dataType:
            for desc_wps_output in self.proc_desc.processOutputs:
                if desc_wps_output.identifier == output.identifier:
                    return desc_wps_output.dataType
        # Also the datatype is not always stripped correctly so do the job here
        return output.dataType.split(':')[-1]

    def _jsonify_output(self, output):
        """
        Utility method to jsonify an output element.
        """
        json_output = dict(identifier=output.identifier,
                           title=output.title,
                           dataType=self.get_output_datatype(output))

        # WPS standard v1.0.0 specify that either a reference or a data field has to be provided
        if output.reference:
            json_output['reference'] = output.reference
        else:
            # WPS standard v1.0.0 specify that Output data field has Zero or one value
            json_output['data'] = output.data[0] if output.data else None

        if json_output['dataType'] == 'ComplexData':
            json_output['mimeType'] = output.mimeType
        return json_output

    def _execute(self):
        """
        This is the function doing the actual WPS process call, monitoring its execution and parsing the output.
        :return: Return the data that is send to the downstream PE
        """
        logger.debug("execute with static inputs=%s and dynamic inputs=%s to get outputs=%s",
                     self.static_inputs, self.dynamic_inputs, self.outputs)

        self.wps.headers['machineid'] = ''.join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(16))

        execution = self.wps.execute(
            identifier=self.identifier,
            inputs=self.static_inputs + self.dynamic_inputs,
            output=self.outputs,
            lineage=True)
        self.set_headers(self.data_headers)
        self._monitor_execution(execution)

        # Reset dynamic inputs for the next iteration
        self.dynamic_inputs = []

        execution_result = {self.STATUS_NAME: execution.status,
                            self.STATUS_LOCATION_NAME: execution.statusLocation}

        if execution.isSucceded():
            execution_result[self.OUTPUT_NAME] = \
                [self._jsonify_output(output) for output in execution.processOutputs]

            self.save_result(execution_result)

            result = {}
            # NOTE: only set workflow output if specific output was needed
            for wps_output in self.outputs:
                for output in execution.processOutputs:
                    if wps_output[0] == output.identifier:
                        # outputs from execution structure do not always carry the dataType
                        # so find it from the process description because the _adapt fct needs it
                        if not output.dataType:
                            for desc_wps_output in self.proc_desc.processOutputs:
                                if desc_wps_output.identifier == output.identifier:
                                    output.dataType = desc_wps_output.dataType
                        # Also the datatype is not always stripped correctly so do the job here
                        else:
                            output.dataType = output.dataType.split(':')[-1]

                        # Send directly the wps output object to the downstream PE
                        # Note: output.data is always an array since a wps output is appended to processOutputs[x].data
                        result[wps_output[0]] = output
                        break
            return result
        else:
            failure_msg = '\n'.join(['{0.text}'.
                                    format(ex) for ex in execution.errors])
            raise WorkflowException(failure_msg)

    @staticmethod
    def iter_inputs(inputs):
        """
        Convert dictionary of inputs where the value is either an array or a value to a flat array of tuple (key, value)
        :param inputs: Dictionnary of inputs looking like this {key1: val1, key2: val2, key3: [val3.1, val3.2, val3.3]}
        :return: yield a tuple for each key/value pair looking like this
                 [(key1, val1), (key2, val2), (key3, val3.1), (key3, val3.2), (key3, val3.3)]
        """
        if not inputs:
            return
        for key in inputs:
            if isinstance(inputs[key], list):
                for value in inputs[key]:
                    yield (key, value)
            else:
                yield (key, inputs[key])


class ParallelGenericWPS(GenericWPS):
    """
    Wrap the execution of a WPS process into a dispel4py PE that will be part of a parallel block
    """
    def __init__(self, group_map_pe, max_processes=1, progress_range=None, **kwargs):
        """
        :param group: Parallel group name
        :param max_processes: Maximum number of instances running concurrently
        :param progress_range: Progress range of this task
        :param kwargs: GenericWPS arguments passed as is
        """
        GenericWPS.__init__(self, **kwargs)

        if not progress_range:
            progress_range = [0, 100]

        self.rank = None  # multi_process assign a rank to PE when multiple instances are instantiate
        self.group = group_map_pe.name
        self.numprocesses = max_processes
        self.set_progress_provider(RangeGroupProgress(group_map_pe, progress_range[0], progress_range[1]))

    def _process(self, inputs):
        """
        Implement the GenericPE _process function.

        ParallelGenericWPS do not support more than one linked inputs because their process function will be called
        multiple times but for different mapped jobs. Trying to match 2 linked inputs part of the same job could prove
        to be a headache.
        Since each job only have one input we can launch the wps execution on each process call.

        :param inputs: What has been outputted by the previous task
        :return: None because the outputs are written within the function
        """

        # Assign inputs
        GenericWPS._process(self, inputs)

        # Launch the wps execution
        GenericWPS._postprocess(self)

    def _postprocess(self):
        """
        Override the GenericWPS postprocess function to nothing because
        ParallelGenericWPS execution happens at each process call
        """
        return None

    def monitor(self, message, progress=None, task_name=None):
        """
        Override TaskPE monitor function. See TaskPE.monitor for details.
        Add the process and data id to the task name.
        """
        if not task_name:
            map_idx = self._get_map_idx()
            task_name = '{name}-proc{proc}-data{data}'.format(name=self.name,
                                                              proc='' if self.rank is None else self.rank,
                                                              data='' if map_idx is None else map_idx)
        GenericWPS.monitor(self, message, progress=progress, task_name=task_name)

    def save_result(self, result):
        """
        Override TaskPE save_result function. See TaskPE.save_result for details.
        Add the process and data id to the result dictionary.
        """
        map_idx = self._get_map_idx()
        result.update(dict(data_id=map_idx,
                           process_id=self.rank))
        GenericWPS.save_result(self, result)

    def _get_map_idx(self):
        """
        Returns the map index from the headers for the current data being processed
        """
        try:
            return self.data_headers[DataWrapper.HEADERS_MAP_INDEX]
        except KeyError:
            return None

    def _validate_inputs(self, inputs, linked_inputs):
        """
        Check that given inputs and linked inputs are valid. Also ParallelGenericWPS supports at most one linked input
        :param inputs: static input (value is available now)
        :param linked_inputs: dynamic inputs (value will be obtained from upstream task)
        """
        if len(linked_inputs) > 1:
            raise WorkflowException((
                "Invalid workflow : Task '{task}' of parallel group '{group}' has {nbi} linked inputs but "
                "parallel group tasks support at most one linked input").format(
                    task=self.name,
                    group=self.group,
                    nbi=len(linked_inputs)))
        GenericWPS._validate_inputs(self, inputs, linked_inputs)
