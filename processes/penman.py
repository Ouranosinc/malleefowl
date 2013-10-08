"""
Processes for Penman Monteith relation 
Author: Nils Hempelmann (nils.hempelmann@hzg)
"""

from datetime import datetime, date
import tempfile
import subprocess

from malleefowl.process import WorkerProcess

class PenmanProcess(WorkerProcess):
    """This process calculates the evapotranspiration following the Pennan Monteith equation"""

    def __init__(self):
        # definition of this process
        WorkerProcess.__init__(self, 
            identifier = "de.csc.esgf.penman.worker",
            title="evapotranspiration (Penman-Monteith)",
            version = "0.1",
            metadata= [
                       {"title": "Climate Service Center", "href": "http://www.climate-service-center.de/"}
                      ],
            abstract="Just testing a nice script to calculate the Penman Monteith equation...",
            extra_metadata={
                  'esgfilter': 'variable:tas,variable:sfcWind,variable:ps,variable:rlds,variable:rsds,variable:rlus,variable:rsus,variable:huss,variable:pr,time_frequency:day ',  #institute:MPI-M,time_frequency:day
                  'esgquery': 'variable:tas AND variable:sfcWind AND variable:ps AND variable:rlds AND variable:rsds AND variable:rlus AND variable:rsus AND variable:huss AND variable:pr AND time_frequency:day' # institute:MPI-M 
                  },
            )

            
        # Literal Input Data
        # ------------------

       
        self.output = self.addComplexOutput(
            identifier="output",
            title="Evapotranspiration",
            abstract="Calculated Evapotranspiration following Penman Monteith relation ",
            formats=[{"mimeType":"application/netcdf"}],
            asReference=True,
            )         
            
    def execute(self):
        from Scientific.IO.NetCDF import NetCDFFile
        from os import curdir, path

        # default var names
        huss = 'huss'
        ps = 'ps'
   
        # guess var names of files
        nc_files = self.get_nc_files()
        for nc_file in nc_files: 
            ds = NetCDFFile(nc_file)
            if "tas" in ds.variables.keys():
                nc_tas = nc_file
            elif "sfcwind" in ds.variables.keys():
                nc_sfcwind = nc_file
            elif "rlds" in ds.variables.keys():
                nc_rlds = nc_file
            elif "rsds" in ds.variables.keys():
                nc_rsds = nc_file
            elif "rlus" in ds.variables.keys():
                nc_rlus = nc_file
            elif "rsus" in ds.variables.keys():
                nc_rsus = nc_file
            elif "ps" in ds.variables.keys():
                nc_ps = nc_file
            elif "huss" in ds.variables.keys():
                nc_huss = nc_file
            elif "pr" in ds.variables.keys():
                nc_pr = nc_file
            else:
                raise Exception("input netcdf file has not variable tas|huss|ps")
                     
               
        nc_evspsblpot = path.join(path.abspath(curdir), "nc_evspsblpot")
        self.cmd(cmd=cmd, stdout=True)
       
        result = self.cmd(cmd=["/home/main/sandbox/climdaps/src/Malleefowl/processes/penman.sh", nc_tas, nc_sfcwind, nc_rlds, nc_rlus , nc_rsds, nc_rsus, nc_ps, nc_huss, nc_pr, nc_evspsblpot], stdout=True)
        
        self.status.set(msg="penman done", percentDone=90, propagate=True)
        self.output.setValue( nc_evspsblpot )
        