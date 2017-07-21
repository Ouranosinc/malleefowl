import sys
import json
import copy

from pywps import ComplexOutput
from pywps import LiteralInput, ComplexInput, BoundingBoxInput
from pywps import Format, get_format
from owslib.wps import Input
from owslib.wps import Output

from malleefowl.utils import DataWrapper, auto_list
from malleefowl.pe.task import TaskPE
from malleefowl.pe.generic_wps import ParallelGenericWPS
from malleefowl.exceptions import WorkflowException


class ReducePE(TaskPE):
    """
    Represent the reduce part of a parallel group of a workflow
    The task must accumulate all the data coming from upstream PEs and send the resulting array to the next PE
    """

    # Name of the PE input/output
    REDUCE_INPUT = 'reduce_in'
    REDUCE_OUTPUT = 'reduce_out'

    def __init__(self, name, reduce_input, monitor):
        TaskPE.__init__(self, name, monitor)

        # global will tell dispel4py to send all upstream nodes data to a single instance of ReducePE
        self._add_input(self.REDUCE_INPUT, grouping='global')
        self._add_output(self.REDUCE_OUTPUT)
        self._add_linked_input(self.REDUCE_INPUT, reduce_input)
        self.output = auto_list(list(), default_val=None)
        # Store the upstream task output description
        # (Will be used as required input if no downstream task are connected)
        self.upstream_task_out_desc = None

        # Store the downstream task input description
        # (If available it will be used in priority as required input
        #  so that the json array is filled with the good data type)
        self.downstream_task_in_desc = None

    def get_input_desc(self, input_name):
        """
        Implement TaskPE fct. See TaskPE.get_input_desc for details.
        """
        if input_name == self.REDUCE_INPUT:
            # If a downstream task is connected use directly that input as our. Else set our input desc based on what
            # the upstream task is already sending.
            return self._convert_in_desc(self.downstream_task_in_desc) if self.downstream_task_in_desc else \
                   self._out_to_in_desc(self.upstream_task_out_desc)
        return None

    def get_output_desc(self, output_name):
        """
        Implement TaskPE fct. See TaskPE.get_output_desc for details.
        """
        if output_name == self.REDUCE_OUTPUT:
            return Output(ComplexOutput(self.REDUCE_OUTPUT,
                                        self.REDUCE_OUTPUT,
                                        supported_formats=[get_format('JSON')],
                                        as_reference=False).describe_xml())
        return None

    def try_connect(self, graph, linked_input, downstream_task, downstream_task_input):
        """
        Override TaskPE fct. See TaskPE.try_connect for details.
        The ReducePE uses the downstream task input format to set it's own input format (so keep a ref)
        """

        # Set the supported output description which is the same as the downstream task supported input
        if TaskPE.try_connect(self, graph, linked_input, downstream_task, downstream_task_input):
            self.downstream_task_in_desc = downstream_task.get_input_desc(downstream_task_input)
            return True
        return False

    def connected_to(self, task_input, upstream_task, upstream_task_output):
        """
        Override TaskPE fct. See TaskPE.connected_to for details.
        The ReducePE could uses the upstream task output format to set it's own input format (so keep a ref)
        """
        self.upstream_task_out_desc = upstream_task.get_output_desc(upstream_task_output)

    def _out_to_in_desc(self, out_desc):
        params = dict(identifier=self.REDUCE_INPUT,
                      title=self.REDUCE_INPUT,
                      min_occurs=1,
                      max_occurs=sys.maxint)
        if out_desc.dataType == 'ComplexData':
            params['supported_formats'] = [Format(mime_type=out_desc.defaultValue.mimeType,
                                                  schema=out_desc.defaultValue.schema,
                                                  encoding=out_desc.defaultValue.encoding)]
            return Input(ComplexInput(**params).describe_xml())
        elif out_desc.dataType == 'BoundingBoxData':
            params['crss'] = out_desc.supportedValues
            return Input(BoundingBoxInput(**params).describe_xml())
        else:
            params['data_type'] = out_desc.dataType
            return Input(LiteralInput(**params).describe_xml())

    def _convert_in_desc(self, in_desc):
        in_desc = copy.deepcopy(self.downstream_task_in_desc)
        in_desc.identifier = self.REDUCE_INPUT
        in_desc.title = self.REDUCE_INPUT
        in_desc.minOccurs = 1
        in_desc.maxOccurs = sys.maxint
        return in_desc

    def _process(self, inputs):
        """
        Implement the GenericPE _process function.
        Each input are put into an ordered array to be send in the _postprocess function
        :param inputs: What has been outputted by the previous task
        :return: None because all the inputs need to be accumulated
        """
        for key, value in self._read_inputs(inputs):
            index = self.data_headers[DataWrapper.HEADERS_MAP_INDEX]
            self.output[index] = value

    def _postprocess(self):
        """
        Implement the GenericPE _postprocess function.
        Send the accumulated array as output
        """
        self._check_inputs()

        # Map index is no more relevant: remove it from the data headers
        self.data_headers.pop(DataWrapper.HEADERS_MAP_INDEX, None)

        output = self.get_output_desc(self.REDUCE_OUTPUT)
        output.data.append(json.dumps(self.output.list))
        output.mimeType = output.defaultValue.mimeType
        output.reference = False
        self._send_outputs({self.REDUCE_OUTPUT: output})

    def _get_default_output(self):
        """
        Implement TaskPE fct.
        The reduce task has only one output so set it as the default one.
        """
        return self.REDUCE_OUTPUT

    def _can_connect(self, linked_input, downstream_task, downstream_task_input):
        """
        Implement TaskPE fct. See TaskPE._can_connect for details.
        """
        # Reduce can only connect to downstream tasks which are not an instance of ParallelGenericWPS
        return not isinstance(downstream_task, ParallelGenericWPS)

    def _check_inputs(self):
        """
        Check if all the required inputs have been set int the output list
        """
        for index, output in enumerate(self.output.list):
            if not output:
                msg = 'Workflow cannot complete because of a missing input ' \
                      '(index {index}) in the reduce array of group {task}'.format(index=index,
                                                                                   task=self.name)
                raise WorkflowException(msg)
