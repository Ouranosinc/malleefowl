"""
Processes with cdo commands

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

#from malleefowl.process import WorkerProcess
import malleefowl.process

from cdo import Cdo

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class CDOOperation(malleefowl.process.WorkerProcess):
    """This process calls cdo with operation on netcdf file"""
    def __init__(self):
        malleefowl.process.WorkerProcess.__init__(
            self,
            identifier = "de.dkrz.cdo.operation",
            title = "CDO Operation",
            version = "0.1",
            metadata=[
                {"title":"CDO","href":"https://code.zmaw.de/projects/cdo"},
                ],
            abstract="calling cdo operation ...",
            )


        # operators
        self.operator_in = self.addLiteralInput(
            identifier="operator",
            title="CDO Operator",
            abstract="Choose a CDO Operator",
            default="monmax",
            type=type(''),
            minOccurs=1,
            maxOccurs=1,
            allowedValues=['merge', 'dayavg', 'daymax', 'daymean', 'daymin', 'monmax', 'monmin', 'monmean', 'monavg']
            )

        # netcdf input
        # -------------

        # defined in WorkflowProcess ...

        # complex output
        # -------------

        self.output = self.addComplexOutput(
            identifier="output",
            title="NetCDF Output",
            abstract="NetCDF Output",
            metadata=[],
            formats=[{"mimeType":"application/x-netcdf"}],
            asReference=True,
            )

    def execute(self):
        logger.debug("running cdo operator")
        
        self.status.set(msg="starting cdo operator", percentDone=10, propagate=True)

        nc_files = self.get_nc_files()
        operator = self.operator_in.getValue()

        cdo = Cdo()
        cdo_op = getattr(cdo, operator)

        outfile = self.mktempfile(suffix='.nc')
        cdo_op(input= " ".join(nc_files), output=outfile)
        
        self.status.set(msg="cdo operator done", percentDone=90, propagate=True)
        self.output.setValue( outfile )


class CDOInfo(malleefowl.process.WorkerProcess):
    """This process calls cdo sinfo on netcdf file"""

    def __init__(self):
        malleefowl.process.WorkerProcess.__init__(
            self,
            identifier = "de.dkrz.cdo.sinfo",
            title = "CDO sinfo",
            version = "0.1",
            metadata=[
                {"title":"CDO","href":"https://code.zmaw.de/projects/cdo"},
                ],
            abstract="calling cdo sinfo ...",
            )

        # complex input
        # -------------

        # comes from workflow process ...

        # complex output
        # -------------

        self.output = self.addComplexOutput(
            identifier="output",
            title="CDO sinfo result",
            abstract="CDO sinfo result",
            metadata=[],
            formats=[{"mimeType":"text/plain"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting cdo sinfo", percentDone=10, propagate=True)

        logger.debug('running cdo sinfo')

        cdo = Cdo()

        from os import curdir, path
        nc_files = self.get_nc_files()

        outfile = self.mktempfile(suffix='.txt')
        with open(outfile, 'w') as fp: 
            for nc_file in nc_files:
                sinfo = cdo.sinfo(input=nc_file, output=outfile)
                for line in sinfo:
                    fp.write(line + '\n')
                fp.write('\n\n')

        self.status.set(msg="cdo sinfo done", percentDone=90, propagate=True)

        logger.debug("cdo sinfo done")

        self.output.setValue( outfile )
