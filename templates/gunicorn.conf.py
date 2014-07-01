bind = '${bind}'
workers = 3

# environment
raw_env = ["HOME=${buildout:anaconda-home}/var/lib/pywps", 
           "PYWPS_CFG=${malleefowl_config:output}", 
           "PATH=${buildout:anaconda-home}/bin:${buildout:bin-directory}:/usr/bin:/bin", 
           "GDAL_DATA=${env:gdal_data}"]                                                                                                               

# logging

debug = True
errorlog = '-'
loglevel = 'debug'
accesslog = '-'
