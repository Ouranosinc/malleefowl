import sys
import json
import copy
import urllib2
from cStringIO import StringIO

from owslib.wps import ComplexDataInput
from owslib.wps import printInputOutput
from dispel4py.core import GenericPE

from malleefowl.utils import DataWrapper

import logging
logger = logging.getLogger("PYWPS")


class ProxyPE(GenericPE):
    """
    Pass-through PE that let connect 2 pe by multiple connections using this proxy
    Connecting A to B twice is not allowed but connecting A to B then A to ProxyPE and ProxyPe to B
    is functionality equivalent and let the goal to be reached
    """
    def __init__(self, input_name, output_name):
        GenericPE.__init__(self)

        self.input_name = input_name
        self.output_name = output_name
        self._add_input(input_name)
        self._add_output(output_name)

    def process(self, inputs):
        """
        Simply feed the input as output
        """
        return {self.output_name: inputs[self.input_name]}


class TaskPE(GenericPE):
    """
    Represent a workflow task executed into a Dispel4Py graph as a GenericPE
    """

    # Headers property that travel along the data in the graph
    HEADERS_TASK_NAME = 'task_name'

    def __init__(self, name, monitor):
        GenericPE.__init__(self)

        self.name = name
        self.data_headers = {}
        self.linked_inputs = []

        # External monitor (not to be used directly)
        self._monitor = monitor

    def monitor(self, message, progress=None, task_name=None):
        """
        Monitoring function to signal progress or status changes
        :param message: message to set into PyWPS
        :param progress: new progress
        :param task_name: current task name (could be decorated when the same task is executed concurrently)
        """
        if self._monitor:
            self._monitor.update_status("{name}: {msg}".format(name=task_name or self.name,
                                                               msg=message),
                                        progress)
        else:
            logger.info('STATUS (%s%s) - %s',
                        task_name or self.name,
                        ': %s/100'.format(progress) if progress else '',
                        message)

    def save_result(self, result):
        """
        Must be called when the task ends to persist the result
        :param result: Task result
        """
        self._monitor.save_task_result(self.name, result)

    def raise_exception(self, exception):
        """
        Must be called when the task has to raise an exception to the main process
        :param exception: The exception to raise
        """
        self._monitor.raise_exception(exception)

    def process(self, inputs):
        """
        Override the GenericPE process function to make sure that no exception is raise above this point
        Because tasks are run on their own process an exception will be lost and others process will wait forever
        :param inputs: What has been outputted by the previous task
        :return: Output to send to the downstream task (None if not done with the task and requires post process)
        """
        try:
            return self._process(inputs)
        except Exception as e:
            # Augment the exception message but conserve the full exception stack
            e.args = ('Exception occurs in task "{0}" process : {1}'.format(self.name, str(e)),)
            self.raise_exception(e)
        return None

    def postprocess(self):
        """
        Override the GenericPE postprocess function to make sure that no exception is raise above this point
        Because tasks are run on their own process an exception will be lost and others process will wait forever
        :return: Output to send to the downstream task
        """
        try:
            return self._postprocess()
        except Exception as e:
            # Augment the exception message but conserve the full exception stack
            e.args = ('Exception occurs in task "{0}" process : {1}'.format(self.name, str(e)),)
            self.raise_exception(e)
        return None

    def get_input_desc(self, input_name):
        """
        Must describe the input input_name as a owslib.wps.Input
        :param input_name: One of the WPS input identifier
        :return: An owslib.wps.Input object
        """
        raise NotImplementedError

    def get_output_desc(self, output_name):
        """
        Must describe the output output_name as a owslib.wps.Output
        :param output_name: One of the WPS output identifier
        :return: An owslib.wps.Output object
        """
        raise NotImplementedError

    def connected_to(self, task_input, upstream_task, upstream_task_output):
        """
        This function is called when the current task is connected to an upstream_task
        :param task_input: The input identifier which is connected
        :param upstream_task: The upstream task TaskPE instance
        :param upstream_task_output: The upstream task connected output identifier
        :return: Nothing
        """
        pass

    def try_connect(self, graph, linked_input, downstream_task, downstream_task_input):
        """
        Try to connect the current task to the downstream task
        :param graph: The graph used to make the connection if possible
        :param linked_input: linked_input dictionary provide by the workflow
        :param downstream_task: The downstream task TaskPE instance
        :param downstream_task_input: The downstream task input identifier
        :return: True or False either if the connection succeeded
        """
        # If self is not the required task return False
        if self.name != linked_input['task']:
            return False

        # If we cannot connect to the downstream task return False
        if not self._can_connect(linked_input, downstream_task, downstream_task_input):
            return False

        # Get the task output required (or the default one if not specified in the workflow)
        task_output = linked_input.get('output', self._get_default_output())

        # Warn downstream task that it as been connected
        downstream_task.connected_to(downstream_task_input, self, task_output)

        # Complete the connection of both PE in the graph
        self._connect(task_output, downstream_task, downstream_task_input, graph)
        return True

    def _connect(self, from_connection, to_node, to_connection, graph):
        """
        Make the dispel4py graph connection between 2 nodes.
        Dispel doesn't support multiple connections between 2 nodes so if required we add a proxy node between them for
        additional connections
        :param self: Upstream node
        :param from_connection: Output name of the upstream node
        :param to_node: Downstream node
        :param to_connection: Input name of the downstream node
        :param graph: Target dispel4py graph
        """
        try:
            # Detect if an edge already exists between these 2 nodes
            # (Will throw an exception if the nodes are still unknown)
            is_connected = graph.graph.has_edge(graph.objToNode[self], graph.objToNode[to_node])
        except KeyError:
            # If the nodes are unknown we know for sure that they are not connected
            is_connected = False

        if is_connected:
            # Insert a proxy node between the 2 nodes if an edge already exist keeping the maximum number of connections
            # between any 2 nodes to 1
            proxy_node = ProxyPE(from_connection, to_connection)
            graph.connect(self, from_connection, proxy_node, from_connection)
            graph.connect(proxy_node, to_connection, to_node, to_connection)
        else:
            # Connect as usual for the 1st connection
            graph.connect(self, from_connection, to_node, to_connection)

        self.monitor('Connection completed between {uptask}:{upout} and {downtask}:{downin}'.format(
                     uptask=self.name,
                     upout=from_connection,
                     downtask=to_node.name,
                     downin=to_connection))

    def _get_default_output(self):
        """
        Get a default output identifier for the current task
        :return: The default output or None if an output must be explicitly specified
        """
        return None

    def _can_connect(self, linked_input, downstream_task, downstream_task_input):
        """
        This function is called to know if the current task can be connected to the given downstream task
        :param linked_input: linked_input dictionary provide by the workflow
        :param downstream_task: The downstream task TaskPE instance
        :param downstream_task_input: The downstream task input identifier
        :return: True or False either if the connection is possible
        """
        task_output = linked_input.get('output', self._get_default_output())
        return self.get_output_desc(task_output) is not None

    def _add_linked_input(self, name, linked_input):
        """
        Explicitely declare inputs that can be connected to upstream task
        :param name: Input identifier
        :param linked_input: linked_input dictionary provide by the workflow
        """
        self.linked_inputs.append((name, linked_input))

    def _send_outputs(self, outputs, extra_headers=None):
        """
        Send the given outputs to the downstream PE in the dispel4py graph and
        make sure to add the proper headers that must travel along the data
        :param outputs: The outputs that will be send
        :param extra_headers: Additional headers to send with the data
        """
        if outputs:
            # Copy the current headers value
            data_headers = copy.deepcopy(self.data_headers)

            # Adn add extra headers
            if extra_headers:
                data_headers.update(extra_headers)

            # Make sure that the headers include the upstream task name
            data_headers[TaskPE.HEADERS_TASK_NAME] = self.name

            # Send all the outputs
            for key, value in outputs.items():
                self.monitor('{name} is sending value - [{headers}] {key}:{val}'.format(name=self.name,
                                                                                        headers=data_headers,
                                                                                        key=key,
                                                                                        val=value))
                self.write(key, DataWrapper(payload=value, headers=data_headers))

    def _read_inputs(self, inputs):
        """
        Read the inputs coming from the dispel4py upstream PE and assign them to the current task inputs
        Extract also the headers traveling along the data
        :param inputs: inputs coming from dispel4py upstream PE
        :return: Yield each input read into the expected format
        """
        valid_inputs = [_input[0] for _input in self.linked_inputs]

        for key in inputs.keys():
            if key in valid_inputs:
                # Accumulate in data headers all headers received, so we can send them once finished
                self.data_headers.update(inputs[key].headers)

                # Get the task name from which the data is coming
                linked_input_tasks = {x[1]['task']: x[1] for x in self.linked_inputs if x[0] == key}
                data_task = inputs[key].headers[TaskPE.HEADERS_TASK_NAME]

                # Now we try to do most of the conversion job between these two data types with the knowledge we have
                # and append the new wps input into the list
                for value in self._adapt(input_value=inputs[key].payload,
                                         input_desc=self.get_input_desc(key),
                                         expecting_reference=linked_input_tasks[data_task].get('as_reference', False)):
                    self.monitor('{name} is reading value - [{headers}] {key}:{val}'.format(name=self.name,
                                                                                            headers=inputs[key].headers,
                                                                                            key=key,
                                                                                            val=value))
                    yield (key, value)

    @staticmethod
    def _print_input_output(value):
        """
        Get a complete description of the WPS input or output value
        """
        sys.stdout = my_stdout = StringIO()

        # print to stdout which we capture
        printInputOutput(value)

        sys.stdout = sys.__stdout__
        return my_stdout.getvalue()

    @staticmethod
    def _get_exception(task_name, input_value, input_desc):
        """
        Produce a detailed exception description when 2 tasks cannot agree on the datatype
        """
        details = ("Upstream task '{in_val}' output doesn't produce a compatible format "
                   "for '{in_desc}' input of '{task}'.").format(
            in_val=input_value.identifier,
            in_desc=input_desc.identifier,
            task=task_name)

        more = 'Output :\n{in_val}\nInput :\n{in_desc}'.format(
            in_val=TaskPE._print_input_output(input_value),
            in_desc=TaskPE._print_input_output(input_desc))
        msg = 'Workflow datatype incompatibility error : {details}\n{more}'.format(details=details, more=more)
        return Exception(msg)

    @staticmethod
    def _read_reference(reference):
        """
        Read a WPS reference and return the content
        """
        try:
            return urllib2.urlopen(reference).read()
        except urllib2.URLError:
            # Don't raise exceptions coming from that.
            return None

    def _adapt(self, input_value, input_desc, expecting_reference):
        """
        Try to fit the input_value to the needs of the downstream task input requirements
        This can involve returning the data or its reference, parsing the reference to return its content or even
        loading a json structure (or a reference ot it) and returning its content
        The function will raise an exception if the input_value cannot met the input requirements
        :param input_value: output data as returned by the upstream task.
                            This object come from the WPSExecution.processOutputs array of (ows.wps.Output) object
        :param input_desc: input description as declared by the current task
        :param expecting_reference: indicate if the current task expects the input_value as reference or not
        :return: The data in the required form
                 The data will be feed to the WebProcessingService.execute function which expect an array of inputs
                 where the input can be :
                    - LiteralData inputs are expressed as simple (key,value) tuples where key is the input identifier,
                                  value is the value
                    - ComplexData inputs are expressed as (key, object) tuples, where key is the input identifier,
                                  and the object must contain a 'getXml()' method that returns an XML infoset to be
                                  included in the WPS request
                                  (ows.wps.ComplexDataInput or ows.wps.BoundingBoxDataInput)
        """
        expecting_complex = input_desc.dataType == 'ComplexData'

        output_data = None
        output_datatype = [input_value.dataType, ]

        if input_value.reference:
            # If a reference is available and we expect a reference consider it as the data from this point
            if expecting_reference:
                output_data = input_value.reference

                # Append the string datatype since a reference can be considered as a string too
                output_datatype.append('string')

            # If we expect the data read the reference
            else:
                output_data = self._read_reference(input_value.reference)

        # process output data are append into a list and
        # WPS standard v1.0.0 specify that Output data field has zero or one value
        elif input_value.data:
            output_data = input_value.data[0]

        # At this point raise an exception if we don't have data in wps_output.data
        if not output_data:
            raise TaskPE._get_exception(self.name, input_value, input_desc)

        # Consider the validation completed if the dataType match for non-complex data or
        # if the mimetype match for complex data
        supported_mimetypes = [value.mimeType for value in input_desc.supportedValues] if expecting_complex else []
        if input_desc.dataType in output_datatype and \
           (not expecting_complex or input_value.mimeType in supported_mimetypes):
            return [output_data, ]

        # Remain cases are either datatypes or complex data mimetypes mismatching...

        # Before raising an exception we will check for a specific case that we can handle:
        # json array that could be fed into the downstream wps wanting an array of data.
        # If this specific case is detected we will simply send the json content to the downstream wps without further
        # validation since the json content type cannot be verified.
        take_array = input_desc.maxOccurs > 1
        if take_array and 'ComplexData' in output_datatype and input_value.mimeType == 'application/json':
            # If the json data is referenced and hasn't already been read, read it now
            if input_value.reference and expecting_reference:
                output_data = self._read_reference(input_value.reference)

            json_data = json.loads(output_data)
            if isinstance(json_data, list):
                array = []
                for value in json_data:
                    # Be a good guy and set the mimeType to something expected...
                    array.append(
                        ComplexDataInput(value,
                                         mimeType=input_desc.supportedValues[0]) if expecting_complex else value)
                return array

        # Cannot do anything else
        raise TaskPE._get_exception(self.name, input_value, input_desc)
