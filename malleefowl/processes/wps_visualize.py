import os
import re
import json
import netCDF4
import dateutil
from urlparse import urlparse

from pywps import Process
from pywps import LiteralInput
from pywps import ComplexOutput
from pywps import Format
from pywps.app.Common import Metadata
from pavics import nctime
from pavics.netcdf import guess_main_variable
from pavics.catalog import variables_default_min_max
from malleefowl import config
from malleefowl.utils import get_auth_cookie

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
            LiteralInput('aggregate', 'Aggregate',
                         data_type='boolean',
                         abstract="If flag is set then all input resources will be merged if possible.",
                         min_occurs=0,
                         max_occurs=1,
                         default='0',
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

        tredds_url = config.thredds_url().strip('/')
        opendap_host_url = tredds_url.replace('fileServer', 'dodsC')
        wms_mapping = config.viz_mapping('wms')
        opendap_mapping = config.viz_mapping('opendap')
        urls = [resource.data for resource in request.inputs['resource']]
        docs = []
        temp_docs = []
        response_data = dict(response=dict(numFound=len(urls), start=0, docs=docs))
        for url in urls:
            wms_url = self.map_url(url, wms_mapping)
            opendap_url = self.map_url(url, opendap_mapping)
            if wms_url and opendap_url:
                temp_docs.append(dict(type='FileAsAggregate',
                                      wms_url=[wms_url,],
                                      opendap_url=[opendap_url,]))
            else:
                raise VisualizeError('Source host is unknown : {0}'.format(url))

        cookie = get_auth_cookie(request)
        daprc_fn = '.daprc'
        auth_cookie_fn = 'auth_cookie'
        if cookie:
            with open(daprc_fn, 'w') as f:
                f.write('HTTP.COOKIEJAR = auth_cookie')

        for url, doc in zip(urls, temp_docs):
            opendap_url = doc['opendap_url'][0]

            if cookie:
                with open(auth_cookie_fn, 'w') as f:
                    for key, value in cookie.items():
                        f.write('{domain}\t{access_flag}\t{path}\t{secure}\t{expiration}\t{name}\t{value}\n'.format(
                            domain=urlparse(opendap_url).hostname,
                            access_flag='FALSE',
                            path='/',
                            secure='FALSE',
                            expiration=0,
                            name=key,
                            value=value))

            try:
                nc = netCDF4.Dataset(opendap_url, 'r')
            except:
                raise VisualizeError('Cannot access source dataset : {0}'.format(opendap_url))

            (datetime_min, datetime_max) = nctime.time_start_end(nc)
            if datetime_min:
                # Time zones are not supported by that function...
                # Plus solr only supports UTC:
                # https://lucene.apache.org/solr/guide/6_6/working-with-dates.html
                doc['datetime_min'] = [nctime.nc_datetime_to_iso(datetime_min, force_gregorian_date=True) + 'Z', ]
            if datetime_max:
                doc['datetime_max'] = [nctime.nc_datetime_to_iso(datetime_max, force_gregorian_date=True) + 'Z', ]
            var_name = guess_main_variable(nc)
            min_max = variables_default_min_max.get(var_name, (0, 1, 'default'))
            doc['variable'] = var_name
            doc['variable_min'] = min_max[0]
            doc['variable_max'] = min_max[1]
            doc['variable_palette'] = min_max[2]
            # Remove server part
            url_trailing_part = doc['opendap_url'][0][len(opendap_host_url) + 1:]
            # Remove file part before joining directory by a dot
            doc['dataset_id'] = '.'.join(url_trailing_part.split('/')[:-1])
            # Aggregate title use the file name until they are merged, at which point it use the dataset_id
            doc['aggregate_title'] = url_trailing_part.split('/')[-1]
            nc.close()

        if cookie:
            os.remove(daprc_fn)
            os.remove(auth_cookie_fn)

        if request.inputs['aggregate'][0].data:
            def overlap(min1, max1, min2, max2):
                """
                Detect overlapping between two time range min1 to max1 and min2 to max2
                """
                dt_min1 = dateutil.parser.parse(min1)
                dt_max1 = dateutil.parser.parse(max1)
                dt_min2 = dateutil.parser.parse(min2)
                dt_max2 = dateutil.parser.parse(max2)
                return dt_min1 <= dt_max2 and dt_min2 <= dt_max1

            def search_matching(doc, docs):
                """
                Return the document in docs matching the attributes of the doc if it exists
                """
                for ref_doc in docs:
                    if doc['dataset_id'] == ref_doc['dataset_id'] and \
                        doc['variable'] == ref_doc['variable'] and \
                        not any(overlap(doc['datetime_min'][0], doc['datetime_max'][0], dt_min, dt_max) \
                                for dt_min, dt_max in zip(ref_doc['datetime_min'], ref_doc['datetime_max'])):
                        return ref_doc


            for doc in temp_docs:
                ref_doc = search_matching(doc, docs)
                if ref_doc:
                    for idx, dt in enumerate(ref_doc['datetime_min']):
                        if dateutil.parser.parse(dt) > dateutil.parser.parse(doc['datetime_min'][0]):
                            ref_doc['datetime_min'].insert(idx, doc['datetime_min'][0])
                            ref_doc['datetime_max'].insert(idx, doc['datetime_max'][0])
                            ref_doc['opendap_url'].insert(idx, doc['opendap_url'][0])
                            ref_doc['wms_url'].insert(idx, doc['wms_url'][0])
                            break
                    # doc dt_min is not less than any dt currently in doc_ref
                    else:
                        ref_doc['datetime_min'].append(doc['datetime_min'][0])
                        ref_doc['datetime_max'].append(doc['datetime_max'][0])
                        ref_doc['opendap_url'].append(doc['opendap_url'][0])
                        ref_doc['wms_url'].append(doc['wms_url'][0])
                    ref_doc['type'] = 'Aggregate'
                    ref_doc['aggregate_title'] = ref_doc['dataset_id']
                else:
                    docs.append(doc)
            response_data['response']['numFound'] = len(docs)

        else:
            for doc in temp_docs:
                docs.append(doc)

        with open('out.json', 'w') as fp:
            json.dump(obj=response_data, fp=fp, indent=4, sort_keys=True)
            response.outputs['output'].file = fp.name

        response.update_status("conversion done", 100)
        return response
