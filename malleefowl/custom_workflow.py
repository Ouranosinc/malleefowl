"""
Run custom workflow without prior knowledge of the underlying component except the fact that they are WPS
The workflow must have the following structure:

Dict of 2 elements :

* name : Workflow name
* tasks : Array of workflow task, each describe by a dict :

  * name : Unique name given to each workflow task
  * url : Url of the WPS provider
  * identifier : Identifier of a WPS process
  * inputs : Array of static input required by the WPS process, each describe by a 2 elements array :

    * Name of the input
    * Value of the input

  * linked_inputs : Array of dynamic input required by the WPS process and obtained by the output of other tasks,
                    each describe by a 2 elements array :

    * Name of the input
    * Provenance of the input, describe by a dict :

      * task : Name of the task from which this input must come from
      * output : Name of the task output that will be linked
      * as_reference : Specify the required form of the input(1) [True: Expect an URL to the input,
                                                                 False: Expect the data directly]

  * progress_range : 2 elements array defining the overall progress range of this task :

    * Start
    * End

(1) The workflow executor is able obviously to assign a reference output to an expected reference input and
a data output to an expected data input but will also be able to read the value of a reference output to send the
expected data input. However, a data output linked to an expected reference input will yield to an exception.

Example:

.. code-block:: json

    {
        "name": "Subsetting workflow",
        "tasks": [
            {
                "name": "Downloading",
                "url": "http://localhost:8091/wps",
                "identifier": "thredds_download",
                "inputs": [["url", "http://localhost:8083/thredds/catalog/birdhouse/catalog.xml"]],
                "progress_range": [0, 50]
            },
            {
                "name": "Subsetting",
                "url": "http://localhost:8093/wps",
                "identifier": "subset_WFS",
                "inputs": [["typename", "ADMINBOUNDARIES:canada_admin_boundaries"],
                           ["featureids", "canada_admin_boundaries.5"],
                           ["mosaic", "False"]],
                "linked_inputs": [["resource", { "task": "Downloading",
                                                 "output": "output",
                                                 "as_reference": False}],],
                "progress_range": [50, 100]
            },
        ]
    }

"""

import argparse
from multiprocessing import Manager

from dispel4py.new import multi_process
from dispel4py.workflow_graph import WorkflowGraph

from malleefowl.pe.map import MapPE
from malleefowl.pe.reduce import ReducePE
from malleefowl.pe.generic_wps import GenericWPS, ParallelGenericWPS

# If the xml document is unavailable after 5 attempts consider that the process has failed
XML_DOC_READING_MAX_ATTEMPT = 5


def run(workflow, monitor=None, headers=None):
    """
    Run the given workflow
    :param workflow: json structure describing the workflow
    :param monitor: monitor callback to receive messages and progress
    :param headers: Headers to use when making a request to the WPS
    :return: A summary of the execution which is a list of all task's xml status
    """

    # Create WPS processes and append them in a task array
    tasks = []

    if 'tasks' in workflow:
        for task in workflow['tasks']:
            tasks.append(GenericWPS(headers=headers, monitor=monitor, **task))

    if 'parallel_groups' in workflow:
        for group in workflow['parallel_groups']:
            map_pe = MapPE(name=group['name'],
                           input=group['map'],
                           monitor=monitor)
            reduce_pe = ReducePE(name=group['name'],
                                 input=group['reduce'],
                                 monitor=monitor)
            tasks.extend([map_pe, reduce_pe])
            for task in group['tasks']:
                tasks.append(ParallelGenericWPS(group_map_pe=map_pe,
                                                max_processes=group['max_processes'],
                                                headers=headers,
                                                monitor=monitor,
                                                **task))

    # Connect each task PE in the dispel graph using the linked inputs information
    # (raise an exception if some connection cannot be done)
    graph = WorkflowGraph()

    for task in tasks:
        # Try to find the referenced PE for each of the linked inputs
        for linked_input in task.linked_inputs:
            found_linked_input = False

            # Loop in the task array searching for the linked task
            for source_task in tasks:
                if source_task.try_connect(graph, linked_input[1], task, linked_input[0]):
                    found_linked_input = True
                    break

            # Unfortunately the linked input has not been resolved, we must raise an exception for that
            if not found_linked_input:
                raise Exception(
                    'Cannot build workflow graph : Task "{task}" has an unknown linked input : {input}'.format(
                        task=task.name,
                        input=str(linked_input[1])))

    # Search for the 'source' pe (which have no inputs) and count the required number of processors
    source_pe = {}
    required_num_proc = 0
    for task in tasks:
        required_num_proc += task.numprocesses
        if not task.linked_inputs:
            source_pe[task] = [{}]

    # Run the graph
    try:
        args = argparse.Namespace(num=required_num_proc, simple=False)
        result = multi_process.process(graph, inputs=source_pe, args=args)
    except Exception as e:
        # Augment the exception message but conserve the full exception stack
        e.args = ('Cannot run the workflow graph : {0}'.format(str(e)),)
        raise

    summary = {}
    for task in tasks:
        try:
            summary[task.name] =\
                {'status_location': result.get((task.id, task.STATUS_LOCATION_NAME))[0],
                 'status': result.get((task.id, task.STATUS_NAME))[0]}
        # Map/Reduce tasks has no status location property, continue silently on exception for that
        # TODO: Get the proper exception type
        except Exception as _:
            continue
    return summary
