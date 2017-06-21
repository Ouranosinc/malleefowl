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
    Keep the progress of each mapped thread in a list
    """
    def __init__(self):
        self.list = auto_list(Manager().list(), default_val=0)

    # The progress list must be shared by every worker using this monitor
    def __deepcopy__(self, memo):
        return copy.copy(self)


class MapPE(TaskPE):
    MAP_INPUT = 'map_in'
    MAP_OUTPUT = 'map_out'

    def __init__(self, name, input, monitor):
        TaskPE.__init__(self, name, monitor)

        self.static_input_list = None
        self._add_output(self.MAP_OUTPUT)
        if isinstance(input, dict):
            self._add_input(self.MAP_INPUT)
            self._add_linked_input(self.MAP_INPUT, input)
        elif isinstance(input, list):
            self.static_input_list = input
        else:
            msg = ('Workflow cannot complete because the group {task} required as input either'
                   ' a task reference or an array of values').format(task=self.name)
            raise Exception(msg)
        self.output_desc = None
        self.progress_list = ProgressList()

    def try_connect(self, graph, linked_input, downstream_task, downstream_task_input):
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
        if input_name == self.MAP_INPUT:
            return Input(ComplexInput(self.MAP_INPUT,
                                      self.MAP_INPUT,
                                      supported_formats=[get_format('JSON')],
                                      min_occurs=1,
                                      max_occurs=sys.maxint).describe_xml())
        return None

    def get_output_desc(self, output_name):
        if output_name == self.MAP_OUTPUT:
            return self.output_desc
        return None

    def _process(self, inputs):
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
        is_complex = self.output_desc.dataType == 'ComplexData'

        # Start with the maximum value so that the monitoring know how much data has to be processed
        for rev_idx, val in enumerate(reversed(input_list)):
            idx = len(input_list) - rev_idx - 1
            output = copy.deepcopy(self.output_desc)
            output.data.append(val)
            self._send_outputs({self.MAP_OUTPUT: output}, extra_headers={DataWrapper.HEADERS_MAP_INDEX: idx})
        return None

    def _get_default_output(self):
        return self.MAP_OUTPUT

    def _can_connect(self, linked_input, downstream_task, downstream_task_input):
        # Map can only connect to downstream tasks which are instances of ParallelGenericWPS and
        # part of the current group
        return isinstance(downstream_task, ParallelGenericWPS) and downstream_task.group == self.name
