import json

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexOutput
from pywps import Format
from pywps.app.Common import Metadata

from malleefowl.download import download_files
from malleefowl.persist import persist_files

import logging
LOGGER = logging.getLogger("PYWPS")


class Persist(Process):

    def __init__(self):
        inputs = [
            LiteralInput('resource', 'Resource',
                         data_type='string',
                         abstract="URL of your resource.",
                         min_occurs=1,
                         max_occurs=1024,
                         ),
            LiteralInput('location', 'Location',
                         data_type='string',
                         abstract="Where to persist the resource. (Can contain facet placeholders {facet_X})",
                         min_occurs=1,
                         max_occurs=1,
                         ),
            LiteralInput('default_facets', 'Default Facets',
                         data_type='string',
                         abstract="{key: value, key2: value2} json dictionary of facets",
                         min_occurs=0,
                         max_occurs=1,
                         ),
            LiteralInput('overwrite', 'Overwrite',
                         data_type='boolean',
                         abstract="Allow to overwrite existing resource. "
                                  "Otherwise an exception is thrown for existing resources.",
                         default=True,
                         ),
        ]
        outputs = [
            ComplexOutput('output', 'Persisted files',
                          abstract="Json document with list of persisted files with file url.",
                          as_reference=False,
                          supported_formats=[Format('application/json')]),
        ]

        super(Persist, self).__init__(
            self._handler,
            identifier="persist",
            title="Persist files",
            version="0.1",
            abstract="Persist files to a structured data directory and provides file list as json document.",
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
        response.update_status("starting persist ...", 0)

        urls = [resource.data for resource in request.inputs['resource']]
        location = request.inputs['location'][0].data
        try:
            default = json.loads(request.inputs['default_facets'][0].data)
        except (ValueError, KeyError):
            default = None
        overwrite = request.inputs['overwrite'][0].data

        LOGGER.debug("downloading urls: %s", len(urls))

        if 'X-X509-User-Proxy' in request.http_request.headers:
            credentials = request.http_request.headers['X-X509-User-Proxy']
            LOGGER.debug('Using X509_USER_PROXY.')
        else:
            credentials = None
            LOGGER.debug('Using no credentials')

        def monitor(msg, progress):
            LOGGER.info("%s - (%d/100)", msg, progress)
            # Download progress will range between 0 and 90 (Last 10 percent being the final move part)
            response.update_status(msg, 0.9 * progress)

        files = download_files(
            urls=urls,
            credentials=credentials,
            monitor=monitor)

        p_files = persist_files(files, location, default, overwrite, request.http_request)

        with open('out.json', 'w') as fp:
            json.dump(obj=p_files, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name

        response.update_status("persist done", 100)
        return response
