"""
Processes for ClimDaPs WPS

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import os
import types
import tempfile

from pywps.Process import WPSProcess as PyWPSProcess
from pywps import config

import utils

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)


class WPSProcess(PyWPSProcess):
    """This is the base class for all climdaps wps processes."""

    def __init__(self, identifier, title, version, metadata=[], abstract=""):
        metadata=[
            {"title":"Literal process"},
            ]
        #metadata.append(
        #    {"title":"ClimDaPs", "href":"http://www.dkrz.de"}
        #    )
        #metadata.append(
        #    {"title":"Hardworking Bird Malleefowl", "href":"http://en.wikipedia.org/wiki/Malleefowl"}
        #    )

        PyWPSProcess.__init__(
            self,
            identifier = identifier,
            title = title,
            version = version,
            storeSupported = "true",   # async
            statusSupported = "true",  # retrieve status, needs to be true for async 
            metadata = metadata,
            abstract=abstract,
            grassLocation = False)

    @property
    def cache_path(self):
        return config.getConfigValue("malleefowl","cachePath")

    @property
    def files_path(self):
        return config.getConfigValue("malleefowl","filesPath")

    @property
    def files_url(self):
        return config.getConfigValue("malleefowl","filesUrl")

    @property
    def working_dir(self):
        return os.path.abspath(os.curdir)

    @property
    def thredds_url(self):
        return config.getConfigValue("malleefowl","threddsUrl")

    def mktempfile(self, suffix='.txt'):
        (_, filename) = tempfile.mkstemp(dir=self.working_dir, suffix=suffix)
        return filename

    def sleep(self, secs):
        import time
        time.sleep(secs)

    def show_status(self, msg, percent_done):
        logger.info('STATUS (%d/100) - %s: %s', percent_done, self.identifier, msg)
        self.status.set(msg=msg, percentDone=percent_done, propagate=True)

class SourceProcess(WPSProcess):
     """This is the base class for all source processes."""

     def __init__(self, identifier, title, version, metadata=[], abstract=""):
        wf_identifier = identifier + ".source"
        #metadata.append(
        #    {"title":"C3Grid", "href":"http://www.c3grid.de"},
        #    )

        #logger.debug("init source process %s", wf_identifier)
        
        WPSProcess.__init__(
            self,
            identifier = wf_identifier,
            title = title,
            version = version,
            metadata = metadata,
            abstract=abstract)

        # input: source filter
        # --------------------
        
        self.file_identifier = self.addLiteralInput(
            identifier="file_identifier",
            title="File Identifier",
            abstract="URL, keyword, ...",
            metadata=[],
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        # netcdf output
        # -------------

        self.output = self.addComplexOutput(
            identifier="output",
            title="NetCDF Output",
            abstract="NetCDF Output",
            metadata=[],
            formats=[{"mimeType":"application/x-netcdf"}],
            asReference=True,
            )

class WorkerProcess(WPSProcess):
    """This is the base class for all worker processes."""

    def __init__(self, identifier, title, version, metadata=[], abstract="",
                 extra_metadata={
                     'esgfilter': '',
                     'esgquery': '*'}):
        wf_identifier = identifier + '.worker'
        #metadata.append(
        #    {"title":"C3Grid", "href":"http://www.c3grid.de"},
        #    )

        #logger.debug("init worker process %s", wf_identifier)

        utils.register_process_metadata(wf_identifier, extra_metadata)
        
        WPSProcess.__init__(
            self,
            identifier = wf_identifier,
            title = title,
            version = version,
            metadata = metadata,
            abstract=abstract)

        
        # complex input
        # -------------

        # TODO: needs some work ...
        self.netcdf_url_in = self.addComplexInput(
            identifier="file_identifier",
            title="NetCDF File",
            abstract="NetCDF File",
            metadata=[],
            minOccurs=1,
            maxOccurs=100,
            maxmegabites=5000,
            formats=[{"mimeType":"application/x-netcdf"}],
            )

    def get_nc_files(self):
        nc_files = []
        value = self.netcdf_url_in.getValue()
        if value != None:
            if type(value) == types.ListType:
                nc_files = value
            else:
                nc_files = [value]

        nc_files = map(os.path.abspath, nc_files)
        return nc_files




    


