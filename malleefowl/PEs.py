import copy
from dispel4py.core import GenericPE
from utils import DataWrapper, auto_list
from multiprocessing import Manager


class ProxyPE(GenericPE):
    """
    Pass-through PE that let connect 2 PEs by multiple connections using this proxy
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


class MapPE(GenericPE):
    MAP_INPUT = 'map_in'
    MAP_OUTPUT = 'map_out'

    def __init__(self):
        GenericPE.__init__(self)

        self._add_input(self.MAP_INPUT)
        self._add_output(self.MAP_OUTPUT)

    def process(self, inputs):
        if self.MAP_INPUT in inputs:
            input_list = inputs[self.MAP_INPUT].payload

            #Start with the maximum value so that the monitoring know how much data has to be processed
            for idx, val in enumerate(reversed(input_list)):
                idx = len(input) - idx - 1
                output = DataWrapper(payload=val,
                                     headers={DataWrapper.HEADERS_MAP_INDEX: idx})
                self.write(self.MAP_OUTPUT, output)


class ReducePE(GenericPE):
    REDUCE_INPUT = 'reduce_in'
    REDUCE_OUTPUT = 'reduce_out'

    def __init__(self):
        GenericPE.__init__(self)

        self._add_input(self.REDUCE_INPUT)
        self._add_output(self.REDUCE_OUTPUT)
        self.output = auto_list(list(), default_val=None)

    def process(self, inputs):
        if self.REDUCE_INPUT in inputs:
            self.output[inputs[self.REDUCE_INPUT].headers[DataWrapper.HEADERS_MAP_INDEX]] = inputs[self.REDUCE_INPUT].payload

    def postprocess(self):
        self.write(self.REDUCE_OUTPUT, DataWrapper(payload=self.output.list))


class ProgressList():
    def __init__(self):
        self.list = auto_list(Manager().list(), default_val=0)

    # The progress list must be shared by every worker using this monitor
    def __deepcopy__(self, memo):
        return copy.copy(self)


class RangeProgress():
    def __init__(self):
        self._pstart = 0
        self._pend = 100

    def set_range(self, start_progress=0, end_progress=100):
        self._pstart = start_progress
        self._pend = end_progress

    def set_headers(self, headers):
        pass

    def progress(self, percentCompleted):
        return int(self._pstart + ((self._pend - self._pstart) / 100.0 * percentCompleted))


class RangeMRProgress(RangeProgress):
    def __init__(self):
        RangeProgress.__init__(self)
        self.progress = ProgressList()
        self.MR_idx = 0

    def set_headers(self, headers):
        try:
            self.MR_idx = headers[DataWrapper.HEADERS_MAP_INDEX]
        except KeyError:
            pass

    def progress(self, percentCompleted):
        self.progress.list[self.MR_idx] = percentCompleted
        return RangeProgress.progress(self, sum(self.progress.list) / len(self.progress.list))



class MonitorPE(GenericPE):
    """
    Augment GenericPE with a functionality to scale the current progress to a specific progress range
    """
    def __init__(self, progress_provider):
        GenericPE.__init__(self)

        self._monitor = None
        self._progress_provider = progress_provider

    def set_monitor(self, monitor, start_progress=0, end_progress=100):
        self._monitor = monitor
        self._progress_provider.set_range(start_progress, end_progress)

    def set_headers(self, headers):
        return self._progress_provider.set_headers(headers)

    def progress(self, execution):
        return self._progress_provider.progress(execution.percentCompleted)

