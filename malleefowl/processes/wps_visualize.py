import re
import json

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexOutput
from pywps import Format
from pywps.app.Common import Metadata

from malleefowl import config

import logging
LOGGER = logging.getLogger("PYWPS")


class VisualizeError(Exception):
    pass


class Visualize(Process):

    def __init__(self):
        inputs = [
            LiteralInput('resource', 'Resource',
                         data_type='string',
                         abstract="URL of your resource.",
                         min_occurs=1,
                         max_occurs=1024,
                         ),
        ]
        outputs = [
            ComplexOutput('output', 'WMS Url corresponding to the given resources',
                          abstract="Json document with list of WMS urls.",
                          as_reference=True,
                          supported_formats=[Format('application/json')]),
        ]

        super(Visualize, self).__init__(
            self._handler,
            identifier="visualize",
            title="Visualize via WMS netcdf files",
            version="0.1",
            abstract="Convert netcdf file urls to the corresponding WMS layer urls as json document.",
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
        response.update_status("starting conversion ...", 0)

        mapping = config.viz_mapping()
        urls = [resource.data for resource in request.inputs['resource']]
        viz_urls = []
        for url in urls:
            for src, viz in mapping:
                viz_url, nb_subs = re.subn(src, viz, str(url), 1)
                if nb_subs == 1:
                    viz_urls.append(viz_url)
                    break
            else:
                raise VisualizeError('Source host is unknown : {0}'.format(url))

        with open('out.json', 'w') as fp:
            json.dump(obj=viz_urls, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name

        response.update_status("conversion done", 100)
        return response
