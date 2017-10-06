import re
import json
import netCDF4
import calendar

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexOutput
from pywps import Format
from pywps.app.Common import Metadata
from pavics import nctime
from pavics.netcdf import guess_main_variable
from pavics.catalog import variables_default_min_max
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
                          abstract="Json document with all metadata required for visualization "
                                   "about the given resources.",
                          as_reference=True,
                          supported_formats=[Format('application/json')]),
        ]

        super(Visualize, self).__init__(
            self._handler,
            identifier="visualize",
            title="Visualize via WMS netcdf files",
            version="0.1",
            abstract="Convert netcdf file urls to metadata required for visualization as json document.",
            metadata=[
                Metadata('Birdhouse', 'http://bird-house.github.io/'),
                Metadata('User Guide', 'http://malleefowl.readthedocs.io/en/latest/'),
            ],
            inputs=inputs,
            outputs=outputs,
            status_supported=True,
            store_supported=True,
        )

    def map_url(self, url, mapping):
        for src, viz in mapping:
            url, nb_subs = re.subn(src, viz, str(url), 1)
            if nb_subs == 1:
                return url
        return None

    def _handler(self, request, response):
        response.update_status("starting conversion ...", 0)

        wms_mapping = config.viz_mapping('wms')
        opendap_mapping = config.viz_mapping('opendap')
        urls = [resource.data for resource in request.inputs['resource']]
        docs = []
        response_data = dict(response=dict(numFound=len(urls), start=0, docs=docs))
        for url in urls:
            wms_url = self.map_url(url, wms_mapping)
            opendap_url = self.map_url(url, opendap_mapping)
            if wms_url and opendap_url:
                docs.append(dict(type='Aggregate',
                                 wms_url=[wms_url,],
                                 opendap_url=[opendap_url,]))
            else:
                raise VisualizeError('Source host is unknown : {0}'.format(url))

        for url, doc in zip(urls, docs):

            try:
                opendap_url =  doc['opendap_url'][0]
                nc = netCDF4.Dataset(opendap_url, 'r')
            except:
                continue

            (datetime_min, datetime_max) = nctime.time_start_end(nc)
            if datetime_min:
                # Apparently, calendar.timegm can take dates from irregular
                # calendars. 2003-03-01 & 2003-02-29 (not a valid gregorian date)
                # both return the same result...
                # Not sure what happens to time zones here...
                doc['datetime_min'] = [calendar.timegm(datetime_min.timetuple()), ]
            if datetime_max:
                doc['datetime_max'] = [calendar.timegm(datetime_max.timetuple()), ]
            var_name = guess_main_variable(nc)
            min_max = variables_default_min_max.get(var_name, (0, 1, 'default'))
            doc['variable'] = var_name
            doc['variable_min'] = min_max[0]
            doc['variable_max'] = min_max[1]
            doc['variable_palette'] = min_max[2]
            nc.close()

        with open('out.json', 'w') as fp:
            json.dump(obj=response_data, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name

        response.update_status("conversion done", 100)
        return response
