"""
Processes for visualisation 
Author: Nils Hempelmann (nils.hempelmann@hzg.de)
"""
from datetime import datetime, date
import tempfile
import subprocess
from malleefowl import tokenmgr, utils
from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

#from malleefowl.process import WorkerProcess
import malleefowl.process
class VisualisationProcess(malleefowl.process.WorkerProcess):
    """This process calculates the evapotranspiration following the Pennan Monteith equation"""

    def __init__(self):
        # definition of this process
        malleefowl.process.WorkerProcess.__init__(self, 
            identifier = "de.csc.visualisation",
            title="Visualisation of data",
            version = "0.1",
            metadata= [
                       {"title": "Climate Service Center", "href": "http://www.climate-service-center.de/"}
                      ],
            abstract="Just testing a nice script to visualise some variables",
            #extra_metadata={
                  #'esgfilter': 'variable:tas,variable:evspsbl,variable:hurs,variable:pr',  #institute:MPI-M, ,time_frequency:day
                  #'esgquery': 'variable:tas AND variable:evspsbl AND variable:hurs AND variable:pr' # institute:MPI-M AND time_frequency:day 
                  #},
            )
            
        # Literal Input Data
        # ------------------

        self.variableIn = self.addLiteralInput(
             identifier="variable",
             title="Variable",
             abstract="Variable to be expected in the input files",
             default="tas",
             type=type(''),
             minOccurs=0,
             maxOccurs=1,
             )
       
        self.output = self.addComplexOutput(
            identifier="output",
            title="visualisation",
            abstract="Visualisation of single variables",
            formats=[{"mimeType":"application/html"}],
            asReference=True,
            )         
            
    def execute(self):
        
        from netCDF4 import Dataset
        from os import curdir, path
        from datetime import datetime
        import numpy as np
        import cdo 
        from bokeh.plotting import *
        
        cdo = cdo.Cdo()

        ncfiles = self.get_nc_files()
        var = self.variableIn.getValue()

        self.show_status('ncfiles and var : %s , %s ' % (ncfiles, var), 7)
        
        # define bokeh 
        output_file("output.html")
        save()
        
        self.show_status('output_file created:' , 5)
        
        hold()
        figure(x_axis_type = "datetime", tools="pan,wheel_zoom,box_zoom,reset,previewsave")
      
        for nc in ncfiles:
            
            self.show_status('looping files : %s ' % (nc), 10)
            # get timestapms
            rawDate = cdo.showdate(input= nc) # ds.variables['time'][:]
            strDate = [elem.strip().split('  ') for elem in rawDate]
            dt = [datetime.strptime(elem, '%Y-%m-%d') for elem in strDate[0]]
            # get vaules
            ds=Dataset(nc)
            data = np.squeeze(ds.variables[var][:])
            meanData = np.mean(data,axis=1)
            ts = np.mean(meanData,axis=1)
            
            # plot into current figure
            line( dt,ts ) # , legend= nc 
            save()
           
        self.show_status('timesseries lineplot done.' , 50)
        
        legend().orientation = "bottom_left"
        curplot().title = "Field mean of %s " % var  
        grid().grid_line_alpha=0.3

        window_size = 30
        window = np.ones(window_size)/float(window_size)
        
        save()
        hold('off')
        
        figure(x_axis_type = "datetime", tools="pan,wheel_zoom,box_zoom,reset,previewsave")
        
        hold()
        
        count = 0
        
        for nc in ncfiles:
            
            self.show_status('looping files : %s ' % (nc), 55)
            # get timestapms
            rawDate = cdo.showdate(input= nc) # ds.variables['time'][:]
            strDate = [elem.strip().split('  ') for elem in rawDate]
            dt = [datetime.strptime(elem, '%Y-%m-%d') for elem in strDate[0]]
            # get vaules
            ds=Dataset(nc)
            data = np.squeeze(ds.variables[var][:])
            meanData = np.mean(data,axis=1)
            ts = np.mean(meanData,axis=1)
            
            # merge to matirx
            
            if (count == 0 ):
                ma = np.append([dt], [ts] , axis = 0 )               
            else :
                #ma = np.append(ma,[ts],axis = 0 )
                # add row for next ensemble menber
                ma = np.append(ma,[(np.empty((len(ma[0,:])))*np.NAN)],axis = 0 )
                # check date and add value
            
            self.show_status('time series to matix  %s' % count , 72)
            
            for i in dt :
                if i in ma[0,:]:
                    x,y = np.where(ma == i)
                    ma[-1,y] = ts[y]
                    self.show_status('date exist', 73)
                else :
                    self.show_status('date not exits', 73)
                    col = (np.empty((len(ma[:,0])))*np.NAN)
                    ma = np.c_[ma,col]
                    ma[0,-1] = i
                    ma[-1,-1] = float(ts[y])
                    self.show_status('col added ', 75)
                        
            self.show_status('fill Vaules %s' % ts[y] , 75)
            count +=1 
            self.show_status('looping uncertainty plot %s' % count , 75)
        
        
        # Sorting the dates  
        #ma = ma[ma[0,:].argsort()]
        
        # calulate mean and percentiles:  
        self.show_status('matrix created ' , 80)
        
        # mask NANs
        
        #mdat = np.ma.masked_array(ma [1:,:],np.isnan(ma [1:,:]))
        
        ens_mean = np.mean(ma [1:,:] ,axis=0)
        self.show_status('mean created ' , 80)
        ens_min = np.min(ma [1:,:] ,axis=0) # np.nanmin
        self.show_status('min created ' , 80)
        ens_max = np.max(ma [1:,:] ,axis=0)
        self.show_status('max created ' , 80)
        
        #per5 = np.percentile(mdat[1:,:], 5, axis=0, out=None )
        #per95 = np.percentile(mdat[1:,:], 95, axis=0, out=None )
        
        #ens_mean = np.mean(ma[1:,:] , axis = 0, out=None )
        #per5 = np.percentile(ma[1:,:], 5, axis=0, out=None )
        #per95 = np.percentile(ma[1:,:], 95, axis=0, out=None )
        
        self.show_status('mean and percentiles done ', 80)

        #segment(dt , (ens_min-10) , dt, (ens_max+30) , color='grey' )
        #xs = np.arry[[dt],[ens_min]]
        #ys = np.arry[[dt],[ens_max]]
        x = []
        y = []
        x = np.append(dt,dt)
        y = np.append(ens_min,ens_max)

        patch(x,y, color='grey', alpha=0.8, line_color=None)
        
        
        #line (dt, ens_min , color='grey')
        line (dt, ens_mean , color='red')
        #line (dt, ens_max , color='grey')
        
        curplot().title = "Mean and Uncertainty of  %s " % var  
        save()
        hold('off')
        
        self.show_status('visualisation done', 99)
        self.output.setValue( 'output.html' )

