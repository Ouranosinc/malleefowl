"""
Processes with cdo commands

Author: Carsten Ehbrecht (ehbrecht@dkrz.de)
"""

import types
import tempfile

from malleefowl.process import WorkflowProcess

class CDOOperation(WorkflowProcess):
    """This process calls cdo with operation on netcdf file"""
    def __init__(self):
        WorkflowProcess.__init__(
            self,
            identifier = "de.dkrz.cdo.operation",
            title = "cdo operation",
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
            allowedValues=['monmax', 'monmin', 'monmean', 'monavg']
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
        self.status.set(msg="starting cdo operator", percentDone=10, propagate=True)

        from os import curdir, path
        nc_filename = path.abspath(self.netcdf_in.getValue(asFile=False))
        operator = self.operator_in.getValue()
        self.message(msg='input netcdf = %s' % (nc_filename), force=True)
        self.message(msg='cdo operator = %s' % (operator), force=True)

        (_, out_filename) = tempfile.mkstemp(suffix='.nc')
        try:
            self.cmd(cmd=["cdo", operator, nc_filename, out_filename], stdout=True)
        except:
            self.message(msg='cdo failed', force=True)

        self.status.set(msg="cdo operator done", percentDone=90, propagate=True)
        self.output.setValue( out_filename )


class CDOInfo(WorkflowProcess):
    """This process calls cdo sinfo on netcdf file"""

    def __init__(self):
        WorkflowProcess.__init__(
            self,
            identifier = "de.dkrz.cdo.sinfo",
            title = "cdo sinfo",
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

        from os import curdir, path
        nc_filename = path.abspath(self.netcdf_in.getValue(asFile=False))
        self.message(msg='nc_filename = %s' % (nc_filename), force=True)

        result = ''
        try:
            result = self.cmd(cmd=["cdo", "sinfo", nc_filename], stdout=True)
        except:
            pass

        self.status.set(msg="cdo sinfo done", percentDone=90, propagate=True)

        (_, out_filename) = tempfile.mkstemp(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(result)
            fp.close()
            self.output.setValue( out_filename )
