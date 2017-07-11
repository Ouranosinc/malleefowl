from malleefowl.utils import DataWrapper
from malleefowl.pe.task import TaskPE


class RangeProgress:
    """
    Provide a progress mapper converting a task progress into a global progress based on the task progress range
    """
    def __init__(self, start_progress=0, end_progress=100):
        self._start = start_progress
        self._end = end_progress

    def set_headers(self, headers):
        """
        Not used. See RangeGroupProgress for details.
        """
        pass

    def progress(self, percent_completed):
        """
        Return a global progress in the range [self._start - self._end]
        :param percent_completed: Task progress [0-100]
        :return: The global progress
        """
        return int(self._start + ((self._end - self._start) / 100.0 * percent_completed))


class RangeGroupProgress(RangeProgress):
    """
    Extend the RangeProgress class for task being part of a group
    They use a progress list shared by each group task that track the progress of each thread and
    return the average progress of them
    """
    def __init__(self, group_map_pe, start_progress=0, end_progress=100):
        RangeProgress.__init__(self, start_progress, end_progress)
        self.progress_list = group_map_pe.progress_list
        self.map_idx = 0

    def set_headers(self, headers):
        """
        The task set the current data headers so that we know on which execution thread we are
        :param headers: Data headers
        """
        try:
            self.map_idx = headers[DataWrapper.HEADERS_MAP_INDEX]
        except KeyError:
            pass

    def progress(self, percent_completed):
        """
        Return a global progress in the range [self._start - self._end] based on the average progress of each
        execution threads
        :param percent_completed: Task progress [0-100]
        :return: The global progress
        """
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
        """
        Set a progress provider of RangeProgress type (or extending it)
        """
        self._progress_provider = provider

    def set_headers(self, headers):
        """
        Forward the current data header to the progress provider
        """
        return self._progress_provider.set_headers(headers)

    def progress(self, execution):
        """
        Return the global progress based on the current WPS execution structure
        """
        return self._progress_provider.progress(execution.percentCompleted)

    def get_input_desc(self, input_name):
        raise NotImplementedError

    def get_output_desc(self, output_name):
        raise NotImplementedError
