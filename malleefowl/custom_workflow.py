"""
Run custom workflow without prior knowledge of the underlying component except the fact that they are WPS
The workflow must respect the schema described by "WORKFLOW_SCHEMA"

Example:

.. code-block:: json

    {
        "name": "workflow_demo_1",
        "tasks": [
            {
                "name": "Downloading",
                "url": "http://localhost:8091/wps",
                "identifier": "thredds_download",
                "inputs": {
                    "url": "http://localhost:8083/thredds/catalog/birdhouse/catalog.xml"
                },
                "progress_range": [0, 40]
            }
        ],
        "parallel_groups": [
            {
                "name": "SubsetterGroup",
                "max_processes": 2,
                "map": {
                    "task": "Downloading",
                    "output": "output",
                    "as_reference": false
                },
                "reduce": {
                    "task": "Subsetting",
                    "output": "ncout",
                    "as_reference": true
                },
                "tasks": [
                    {
                        "name": "Subsetting",
                        "url": "http://localhost:8093/wps",
                        "identifier": "subset_WFS",
                        "inputs": {
                            "typename": "ADMINBOUNDARIES:canada_admin_boundaries",
                            "featureids": "canada_admin_boundaries.5",
                            "mosaic": "False"
                        },
                        "linked_inputs": {
                            "resource": {
                                "task": "SubsetterGroup"
                            }
                        },
                        "progress_range": [40, 100]
                    }
                ]
            }
        ]
    }



"""

import argparse
import jsonschema
from dispel4py.new import multi_process
from dispel4py.workflow_graph import WorkflowGraph

from malleefowl.pe.map import MapPE
from malleefowl.pe.reduce import ReducePE
from malleefowl.pe.generic_wps import GenericWPS, ParallelGenericWPS
from malleefowl.exceptions import WorkflowException


# If the xml document is unavailable after 5 attempts consider that the process has failed
XML_DOC_READING_MAX_ATTEMPT = 5

# The schema that must be respected by the submitted workflow
WORKFLOW_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "Workflow",
    "description": "Advanced workflow schema",
    "type": "object",
    "required": ["name"],
    "minProperties": 2,
    "additionalProperties": False,
    "properties": {
        "name": {
            "description": "Workflow name",
            "type": "string"
        },
        "tasks": {
            "description": "Array of workflow task",
            "type": "array",
            "minItems": 1,
            "items": { "$ref": "#/definitions/workflow_task_schema" }
        },
        "parallel_groups": {
            "description": "Array of group of tasks being executed on multiple processes",
            "type": "array",
            "minItems": 1,
            "items": { "$ref": "#/definitions/group_of_task_schema" }
        }
    },
    "definitions": {
        "workflow_task_schema": {
            "description": "Describe a WPS process task",
            "type": "object",
            "required": ["name", "url", "identifier"],
            "additionalProperties": False,
            "properties": {
                "name": {
                    "description": "Unique name given to each workflow task",
                    "type": "string"
                },
                "url": {
                    "description": "Url of the WPS provider",
                    "type": "string"
                },
                "identifier": {
                    "description": "Identifier of a WPS process",
                    "type": "string"
                },
                "inputs": {
                    "description": "Dictionary of inputs that must be fed to the WPS process",
                    "type": "object",
                    "minItems": 1,
                    "patternProperties": {
                        ".*": {
                            "oneOf": [
                                {
                                    "description": "Data that must be fed to this input",
                                    "type": "string"
                                },
                                {
                                    "description": "Array of data that must be fed to this input",
                                    "type": "array",
                                    "minItems": 1,
                                    "items": {
                                        "type": "string"
                                    }
                                }
                            ]
                        }
                    }
                },
                "linked_inputs": {
                    "description": "Dictionary of dynamic inputs that must be fed to the WPS process and obtained by the output of other tasks",
                    "type": "object",
                    "minItems": 1,
                    "patternProperties": {
                        ".*": {
                            "oneOf": [
                                { "$ref": "#/definitions/input_description_schema" },
                                {
                                    "description": "Array of input description that must be fed to this input",
                                    "type": "array",
                                    "minItems": 1,
                                    "items": { "$ref": "#/definitions/input_description_schema" }
                                }
                            ]
                        }
                    }
                },
                "progress_range": {
                    "description": "Progress range to map the whole progress of this task",
                    "type": "array",
                    "minItems": 2,
                    "maxItems": 2,
                    "items": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 100
                    }
                }
            }
        },
        "group_of_task_schema" : {
            "type": "object",
            "description": "Describe a group of tasks to be run concurrently",
            "required": ["name", "max_processes", "map", "reduce", "tasks"],
            "additionalProperties": False,
            "properties": {
                "name": {
                    "description": "Group of task name",
                    "type": "string"
                },
                "max_processes": {
                    "description": "Number of processes to run concurrently to process the data",
                    "type": "number",
                    "minimum": 1
                },
                "map": {
                    "oneOf": [
                        { "$ref": "#/definitions/input_description_schema" },
                        {
                            "description": "Array of data that has to be mapped directly",
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "string"
                            }
                        }
                    ]
                },
                "reduce": { "$ref": "#/definitions/input_description_schema" },
                "tasks": {
                    "description": "Array of workflow task to run concurrently inside the group",
                    "type": "array",
                    "minItems": 1,
                    "items": { "$ref": "#/definitions/workflow_task_schema" }
                }
            }
        },
        "input_description_schema" : {
            "description": "Description of an input source",
            "type": "object",
            "required": ["task"],
            "additionalProperties": False,
            "properties": {
                "task": {
                    "description": "Task name",
                    "type": "string"
                },
                "output": {
                    "description": "Task output name",
                    "type": "string"
                },
                "as_reference": {
                    "description": "Specify if the task output should be obtained as a reference or not",
                    "type": "boolean"
                }
            }
        }
    }
}


def run(workflow, monitor=None, headers=None):
    """
    Run the given workflow
    :param workflow: json structure describing the workflow
    :param monitor: monitor callback to receive messages and progress
    :param headers: Headers to use when making a request to the WPS
    :return: A summary of the execution which is a list of all task's xml status
    """

    try:
        jsonschema.validate(workflow, WORKFLOW_SCHEMA)
    except jsonschema.ValidationError as e:
        raise WorkflowException('The workflow is invalid : {0}'.format(str(e)))

    # Create WPS processes and append them in a task array
    tasks = []

    # Append tasks from the tasks section (but this section is not mandatory)
    if 'tasks' in workflow:
        for task in workflow['tasks']:
            tasks.append(GenericWPS(headers=headers, monitor=monitor, **task))

    # Append tasks from each parallel group of the parallel_groups section (but this section is not mandatory)
    if 'parallel_groups' in workflow:
        for group in workflow['parallel_groups']:
            map_pe = MapPE(name=group['name'],
                           map_input=group['map'],
                           monitor=monitor)
            reduce_pe = ReducePE(name=group['name'],
                                 reduce_input=group['reduce'],
                                 monitor=monitor)
            tasks.extend([map_pe, reduce_pe])

            # Append tasks for each parallel_group (this section is mandatory)
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
                raise WorkflowException(
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
        multi_process.process(graph, inputs=source_pe, args=args)
    except Exception as e:
        # Augment the exception message but conserve the full exception stack
        e.args = ('Cannot run the workflow graph : {0}'.format(str(e)),)
        raise
