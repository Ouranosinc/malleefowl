from pywps.Process import WPSProcess

from malleefowl.process import show_status

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class CrashTestDummy(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "crashtestdummy",
            title = "Crash Test Dummy",
            version = "0.2",
            abstract="See what happens when a process fails.",
            statusSupported=True,
            storeSupported=True,
            )

        self.netcdf_file = self.addComplexInput(
            identifier="resource",
            title="Resource",
            abstract="NetCDF File",
            minOccurs=0,
            maxOccurs=100,
            maxmegabites=5000,
            formats=[{"mimeType":"application/x-netcdf"}],
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="output",
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

    def execute(self):
        show_status(self, "starting ...", 0)

        nc_files = self.getInputValues(identifier='resource')

        outfile = self.mktempfile(suffix='.txt')
        with open(outfile, 'w') as fp:
            import os
            fp.write('PYTHONPATH={0}\n'.format( os.environ.get('PYTHONPATH')) )
            fp.write('num input files={0}\n'.format( len(nc_files)) )
            self.output.setValue( outfile )

        import time
        for i in range(1, 5):
            time.sleep(1)
            self.show_status("Working ...", i*20)

        show_status(self, 'before crash', 90)
        raise Exception('boooomm ... process crashed!')

class Dummy(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "dummy",
            title = "Dummy",
            version = "0.2",
            abstract="Dummy process for testing.",
            statusSupported=True,
            storeSupported=True,
            )

        self.netcdf_file = self.addComplexInput(
            identifier="resource",
            title="Resource",
            abstract="NetCDF File",
            minOccurs=0,
            maxOccurs=100,
            maxmegabites=5000,
            formats=[{"mimeType":"application/x-netcdf"}],
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="output",
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

        self.status_log = self.addComplexOutput(
            identifier="log",
            title="log",
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

    def execute(self):
        show_status(self, "starting ...", 0)

        nc_files = self.getInputValues(identifier='resource')

        outfile = self.mktempfile(suffix='.txt')
        with open(outfile, 'w') as fp:
            import os
            fp.write('PYTHONPATH=%s\n' % (os.environ.get('PYTHONPATH')))
            fp.write('num input files=%s\n' % len(nc_files))
            self.output.setValue( outfile )

        import time
        for i in xrange(1, 5):
            time.sleep(1)
            self.show_status("Working ...", i*20) 

        outfile = self.mktempfile(suffix='.txt')
        with open(outfile, 'w') as fp:
            import os
            fp.write('job done')
            self.status_log.setValue( outfile )

        show_status(self, "done", 100)

        

        
