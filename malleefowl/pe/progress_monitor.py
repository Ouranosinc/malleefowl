import copy
from multiprocessing import Manager

from malleefowl.utils import DataWrapper, auto_list
from malleefowl.pe.task import TaskPE


class RangeProgress:
    def __init__(self, start_progress=0, end_progress=100):
        self._start = start_progress
        self._end = end_progress

    def set_headers(self, headers):
        pass

    def progress(self, percent_completed):
        return int(self._start + ((self._end - self._start) / 100.0 * percent_completed))


class RangeGroupProgress(RangeProgress):
    def __init__(self, group_map_pe, start_progress=0, end_progress=100):
        RangeProgress.__init__(self, start_progress, end_progress)
        self.progress_list = group_map_pe.progress_list
        self.map_idx = 0

    def set_headers(self, headers):
        try:
            self.map_idx = headers[DataWrapper.HEADERS_MAP_INDEX]
        except KeyError:
            pass

    def progress(self, percent_completed):
        self.progress_list.list.resize(self.map_idx + 1, self._start)
        self.progress_list.list[self.map_idx] = RangeProgress.progress(self, percent_completed)
        return sum(self.progress_list.list) / len(self.progress_list.list)


class ProgressMonitorPE(TaskPE):
    """
    Augment GenericPE with a functionality to scale the current progress to a specific progress range
    """
    def __init__(self, name, monitor):
        TaskPE.__init__(self, name, monitor)

        self._progress_provider = None

    def set_progress_provider(self, provider):
        self._progress_provider = provider

    def set_headers(self, headers):
        return self._progress_provider.set_headers(headers)

    def progress(self, execution):
        return self._progress_provider.progress(execution.percentCompleted)

    def get_input_desc(self, input_name):
        raise NotImplementedError

    def get_output_desc(self, output_name):
        raise NotImplementedError
