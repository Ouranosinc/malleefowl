#from malleefowl.process import WorkerProcess
import malleefowl.process
from malleefowl import utils

import os

import logging
log = logging.getLogger(__name__)

class Publish(malleefowl.process.WorkerProcess):
    """Publish netcdf files to thredds server"""
    def __init__(self):
        malleefowl.process.WorkerProcess.__init__(
            self,
            identifier = "de.dkrz.publish",
            title = "Publish NetCDF Files to Thredds Server",
            version = "0.1",
            metadata=[
                {"title":"ClimDaPs","href":"https://redmine.dkrz.de/collaboration/projects/climdaps"},
                ],
            abstract="Publish netcdf files to Thredds server...",
            )

        self.openid_in = self.addLiteralInput(
            identifier = "openid",
            title = "ESGF OpenID",
            abstract = "Enter ESGF OpenID",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )

        self.password_in = self.addLiteralInput(
            identifier = "password",
            title = "OpenID Password",
            abstract = "Enter your Password",
            minOccurs = 1,
            maxOccurs = 1,
            type = type('')
            )
        
        self.basename = self.addLiteralInput(
            identifier="basename",
            title="Basename",
            abstract="Basename of files",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Publisher result",
            abstract="Publisher result",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )
        
    def execute(self):
        self.status.set(msg="starting publisher", percentDone=10, propagate=True)

        esgf_credentials = utils.logon(
            openid=self.openid_in.getValue(), 
            password=self.password_in.getValue())
        
        self.status.set(msg="logon successful", percentDone=20, propagate=True)

        user_id = utils.user_id(self.openid_in.getValue())

        nc_files = self.get_nc_files()

        result = "Published files to thredds server\n"

        outdir = os.path.join(self.files_path, user_id)
        utils.mkdir(outdir)
        
        count = 0
        for nc_file in nc_files:
            outfile = os.path.join(outdir,
                                   self.basename.getValue() + "-" +
                                   os.path.basename(nc_file) + ".nc")
            result = result + outfile + "\n"
            try:
                os.link(os.path.abspath(nc_file), outfile)
                result = result + "success\n"
            except:
                log.error("publishing of %s failed", nc_file)
                result = result + "failed\n"
            count = count + 1
            percent_done = int(20 + 70.0 / len(nc_files) * count)
            self.status.set(msg="%d file(s) published" % count,
                            percentDone=percent_done, propagate=True)

        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(result)
            fp.close()
            self.output.setValue( out_filename )

        self.status.set(msg="publisher done", percentDone=90, propagate=True)
