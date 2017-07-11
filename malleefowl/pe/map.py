import sys
import json
import copy
from multiprocessing import Manager

from pywps import LiteralOutput, ComplexOutput, BoundingBoxOutput
from pywps import ComplexInput
from pywps import Format, get_format
from owslib.wps import Input, Output

from malleefowl.utils import DataWrapper, auto_list
from malleefowl.pe.task import TaskPE
from malleefowl.pe.generic_wps import ParallelGenericWPS


class ProgressList:
    """
    Simple list to track the progress of each execution threads inside a parallel group that is shared by each tasks
    """
    def __init__(self):
        self.list = auto_list(Manager().list(), default_val=0)

    # The progress list must be shared by every worker using this monitor, so avoid deep copy
    def __deepcopy__(self, memo):
        return copy.copy(self)


class MapPE(TaskPE):
    """
   Represent the map part of a parallel group of a workflow
   The task split an array of data so that they can be processed concurrently by the downstream PEs
   """

    # Name of the PE input/output
    MAP_INPUT = 'map_in'
    MAP_OUTPUT = 'map_out'

    def __init__(self, name, map_input, monitor):
        TaskPE.__init__(self, name, monitor)

        self.static_input_list = None
        self._add_output(self.MAP_OUTPUT)

        # If the map_input is a dict, we got a linked input description
        if isinstance(map_input, dict):
            self._add_input(self.MAP_INPUT)
            self._add_linked_input(self.MAP_INPUT, map_input)

        # Else it should be a list of data to be map directly
        elif isinstance(map_input, list):
            self.static_input_list = map_input

        else:
            msg = ('Workflow cannot complete because the group {task} required as input either'
                   ' a task reference or an array of values').format(task=self.name)
            raise Exception(msg)
        self.output_desc = None

        # This is the progress list shared by each of our tasks in which they will set their progress
        self.progress_list = ProgressList()

    def try_connect(self, graph, linked_input, downstream_task, downstream_task_input):
        """
        Override TaskPE fct. See TaskPE.try_connect for details.
        The MapPE uses the downstream task input format to set it's own output format and it's set upon connection
        """

        # Set the supported output description which is the same as the downstream task supported input
        if TaskPE.try_connect(self, graph, linked_input, downstream_task, downstream_task_input):
            down_task_in_desc = downstream_task.get_input_desc(downstream_task_input)
            params = dict(identifier=self.MAP_OUTPUT,
                          title=self.MAP_OUTPUT)
            if down_task_in_desc.dataType == 'ComplexData':
                params['supported_formats'] = [Format(mime_type=down_task_in_desc.defaultValue.mimeType,
                                                      schema=down_task_in_desc.defaultValue.schema,
                                                      encoding=down_task_in_desc.defaultValue.encoding)]
                params['as_reference'] = False
                self.output_desc = Output(ComplexOutput(**params).describe_xml())
                self.output_desc.mimeType = down_task_in_desc.defaultValue.mimeType
            elif down_task_in_desc.dataType == 'BoundingBoxData':
                params['crss'] = down_task_in_desc.supportedValues
                params['as_reference'] = False
                self.output_desc = Output(BoundingBoxOutput(**params).describe_xml())
            else:
                params['data_type'] = down_task_in_desc.dataType
                self.output_desc = Output(LiteralOutput(**params).describe_xml())
            return True
        return False

    def get_input_desc(self, input_name):
        """
        Implement TaskPE fct. See TaskPE.get_input_desc for details.
        """
        if input_name == self.MAP_INPUT:
            return Input(ComplexInput(self.MAP_INPUT,
                                      self.MAP_INPUT,
                                      supported_formats=[get_format('JSON')],
                                      min_occurs=1,
                                      max_occurs=sys.maxint).describe_xml())
        return None

    def get_output_desc(self, output_name):
        """
        Implement TaskPE fct. See TaskPE.get_output_desc for details.
        """
        if output_name == self.MAP_OUTPUT:
            return self.output_desc
        return None

    def _process(self, inputs):
        """
        Implement the GenericPE _process function.
        Each input are send to the map_input_list fct which send each value to the downstream task
        :param inputs: What has been outputted by the previous task
        :return: None because the output are send explicitly inside the function
        """
        if self.static_input_list:
            self._map_input_list(self.static_input_list)
        else:
            for key, value in self._read_inputs(inputs):
                input_list = json.loads(value)
                if isinstance(input_list, list):
                    self._map_input_list(input_list)
                else:
                    msg = ('Workflow cannot complete because the group {task} '
                           'has not receive a json array as input').format(task=self.name)
                    raise Exception(msg)

    def _map_input_list(self, input_list):
        """
        Parse the input list and send each value using the good format and set the mapping index in the header
        :param input_list: List of value to send
        """
        is_complex = self.output_desc.dataType == 'ComplexData'

        # Start with the maximum value so that the monitoring know how much data has to be processed
        for rev_idx, val in enumerate(reversed(input_list)):
            idx = len(input_list) - rev_idx - 1
            output = copy.deepcopy(self.output_desc)
            output.data.append(val)
            self._send_outputs({self.MAP_OUTPUT: output}, extra_headers={DataWrapper.HEADERS_MAP_INDEX: idx})
        return None

    def _get_default_output(self):
        """
        Implement TaskPE fct.
        The map task has only one output so set it as the default one.
        """
        return self.MAP_OUTPUT

    def _can_connect(self, linked_input, downstream_task, downstream_task_input):
        """
        Implement TaskPE fct. See TaskPE._can_connect for details.
        """

        # Map can only connect to downstream tasks which are instances of ParallelGenericWPS and
        # part of the current group
        return isinstance(downstream_task, ParallelGenericWPS) and downstream_task.group == self.name
