import json

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexInput
from pywps import ComplexOutput
from pywps import Format, FORMATS
from pywps.app.Common import Metadata

from malleefowl import download
from malleefowl.utils import get_auth_cookie

class ThreddsDownload(Process):
    def __init__(self):
        inputs = [
            LiteralInput('url', 'URL',
                         data_type='string',
                         abstract="URL of the catalog.",
                         min_occurs=1,
                         max_occurs=1,
                         ),
        ]
        outputs = [
            ComplexOutput('output', 'Downloaded files',
                          abstract="JSON document with list of downloaded files with file url.",
                          as_reference=True,
                          supported_formats=[Format('application/json')]),
        ]

        super(ThreddsDownload, self).__init__(
            self._handler,
            identifier="thredds_download",
            title="Download files from Thredds Catalog",
            version="0.5",
            abstract="Downloads files from Thredds Catalog and provides file list as JSON Document.",
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
        files = download.download_files_from_thredds(
            url=request.inputs['url'][0].data,
            cookie=get_auth_cookie(request),
            monitor=monitor)

        with open('out.json', 'w') as fp:
            json.dump(obj=files, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name
        return response


class ThreddsUrls(Process):
    def __init__(self):
        inputs = [
            LiteralInput('url', 'URL',
                         data_type='string',
                         abstract="URL of the catalog.",
                         min_occurs=1,
                         max_occurs=1,
                         ),
        ]
        outputs = [
            ComplexOutput('output', 'Catalog file urls',
                          abstract="JSON document with list of catalog file url.",
                          as_reference=True,
                          supported_formats=[Format('application/json')]),
        ]

        super(ThreddsUrls, self).__init__(
            self._handler,
            identifier="thredds_urls",
            title="Get files url from Thredds Catalog",
            version="0.5",
            abstract="Get files url from Thredds Catalog and provides file list as JSON Document.",
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

        cookie = get_auth_cookie(request)
        catalog_url = request.inputs['url'][0].data
        urls = download.get_thredds_download_urls(catalog_url, cookie)

        with open('out.json', 'w') as fp:
            json.dump(obj=urls, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name
        return response


class ThreddsOpenDapUrls(Process):
    def __init__(self):
        inputs = [
            LiteralInput('url', 'URL',
                         data_type='string',
                         abstract="URL of the catalog.",
                         min_occurs=1,
                         max_occurs=1,
                         ),
        ]
        outputs = [
            ComplexOutput('output', 'Catalog file opendap urls',
                          abstract="JSON document with list of catalog file opendap url.",
                          as_reference=True,
                          supported_formats=[Format('application/json')]),
        ]

        super(ThreddsOpenDapUrls, self).__init__(
            self._handler,
            identifier="thredds_opendap_urls",
            title="Get files opendap url from Thredds Catalog",
            version="0.5",
            abstract="Get files opendap url from Thredds Catalog and provides file list as JSON Document.",
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

        cookie = get_auth_cookie(request)
        catalog_url = request.inputs['url'][0].data
        urls = download.get_thredds_opendap_urls(catalog_url, cookie)

        with open('out.json', 'w') as fp:
            json.dump(obj=urls, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name
        return response
