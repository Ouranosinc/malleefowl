import yaml

from pywps import Process
from pywps import ComplexInput
from pywps import ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata

from malleefowl import config
from malleefowl.workflow import run


class DispelWorkflow(Process):
    def __init__(self):
        inputs = [
            ComplexInput('workflow', 'Workflow description',
                         abstract='Workflow description in YAML.',
                         metadata=[Metadata('Info')],
                         min_occurs=1,
                         max_occurs=1,
                         supported_formats=[Format('text/yaml')]),
        ]
        outputs = [
            ComplexOutput('output', 'Workflow result',
                          abstract="Workflow result document in YAML.",
                          as_reference=True,
                          supported_formats=[Format('text/yaml')]),
            ComplexOutput('logfile', 'Workflow log file',
                          abstract="Workflow log file.",
                          as_reference=True,
                          supported_formats=[Format('text/plain')]),
        ]

        super(DispelWorkflow, self).__init__(
            self._handler,
            identifier="workflow",
            title="Workflow",
            version="0.7",
            abstract="Runs Workflow with dispel4py.",
            metadata=[
                Metadata('Birdhouse', 'http://bird-house.github.io/'),
                Metadata('User Guide', 'http://malleefowl.readthedocs.io/en/latest/'),
            ],
            inputs=inputs,
            outputs=outputs,
            status_supported=True,
            store_supported=True,
        )

    def _handler(self, request, response):
        def monitor(message, progress):
            response.update_status(message, progress)

        response.update_status("starting workflow ...", 0)

        workflow = yaml.load(request.inputs['workflow'][0].stream)

        workflow_name = workflow.get('name', 'unknown')

        response.update_status("workflow {0} prepared.".format(workflow_name), 0)

        result = run(workflow, monitor=monitor)

        with open('output.txt', 'w') as fp:
            yaml.dump(result, stream=fp)
            response.outputs['output'].file = fp.name
        with open('logfile.txt', 'w') as fp:
            fp.write("workflow log file")
            response.outputs['logfile'].file = fp.name
        response.update_status("workflow {0} done.".format(workflow_name), 100)
        return response


class DummyProcess(Process):
    def __init__(self):
        inputs = [
            ComplexInput('dataset', 'NetCDF File',
                         abstract='You may provide a URL or upload a NetCDF file.',
                         metadata=[Metadata('Info')],
                         min_occurs=1,
                         max_occurs=10,
                         supported_formats=[Format('application/x-netcdf')]),
        ]
        outputs = [
            ComplexOutput('output', 'Output',
                          abstract="Short info about your datasets.",
                          as_reference=True,
                          supported_formats=[Format('text/plain')]),
        ]

        super(DummyProcess, self).__init__(
            self._handler,
            identifier="dummy",
            title="Dummy Process",
            version="1.1",
            abstract="Dummy Process used by Workflow Tests.",
            metadata=[
                Metadata('Birdhouse', 'http://bird-house.github.io/'),
                Metadata('User Guide', 'http://malleefowl.readthedocs.io/en/latest/'),
            ],
            inputs=inputs,
            outputs=outputs,
            status_supported=True,
            store_supported=True,
        )

    def _handler(self, request, response):
        response.update_status("starting ...", 0)
        with open('out.txt', 'w') as fp:
            fp.write('we got {} files.'.format(len(request.inputs['datasets'])))
            response.outputs['output'].file = fp.name
        response.update_status("done", 100)
        return response
