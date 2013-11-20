from malleefowl.process import WPSProcess

from owslib.wms import WebMapService

import json
from datetime import datetime, date
import types

class GetWMSLayers(WPSProcess):
    """Retrieve layers from thredds ncwms service."""

    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "org.malleefowl.wms.layer",
            title = "Get WMS Layers",
            version = "0.1",
            metadata = [],
            abstract = "Get all Layers from Thredds ncWMS Service",
            )

        self.start_in = self.addLiteralInput(
            identifier="start",
            title="Start Date",
            abstract="Start Date: 2006-01-01",
            default="2006-01-01",
            type=type(date(2013,7,11)),
            minOccurs=0,
            maxOccurs=1,
            )

        self.end_in = self.addLiteralInput(
            identifier="end",
            title="End Date",
            abstract="End Date: 2006-12-31",
            default="2006-12-31",
            type=type(date(2013,7,11)),
            minOccurs=0,
            maxOccurs=1,
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="WMS Layers",
            abstract="WMS Layers as JSON",
            metadata=[],
            formats=[{"mimeType":"application/json"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting ...", percentDone=10, propagate=True)

        service_url = self.thredds_url + '/wms/test/cordex-eur-tas-year-pywpsInputoZXCTG.nc'
        wms = WebMapService(service_url, version='1.1.1')

        layers = []
        for layerid,layer in wms.items():
            if layerid in ['lat', 'lon']:
                continue
            timesteps = map(str.strip, layer.timepositions)
            layers.append(dict(name=layer.name, title=layer.title, timesteps=timesteps))

        self.status.set(msg="done", percentDone=90, propagate=True)
        
        out_filename = self.mktempfile(suffix='.txt')
        with open(out_filename, 'w') as fp:
            fp.write(json.dumps(layers))
            fp.close()
            self.output.setValue( out_filename )

class AnimateWMSLayer(WPSProcess):
    """Create gif animation of wms layer for timesteps."""

    def __init__(self):
        WPSProcess.__init__(
            self,
            identifier = "org.malleefowl.wms.animate",
            title = "Animate WMS Layer",
            version = "0.1",
            metadata = [],
            abstract = "Create gif animation of wms wms layer for timesteps.",
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Animated WMS Layer",
            abstract="Animated gif of WMS Layer",
            metadata=[],
            formats=[{"mimeType":"image/gif"}],
            asReference=True,
            )

    def execute(self):
        self.status.set(msg="starting ...", percentDone=10, propagate=True)

        service_url = self.thredds_url + '/wms/test/cordex-eur-tas-year-pywpsInputoZXCTG.nc'
        wms = WebMapService(service_url, version='1.1.1')

        layer = wms.contents['tas']
        timesteps = map(str.strip, layer.timepositions)

        img_filename = self.mktempfile(suffix='.gif')
        img = wms.getmap(layers=['tas'],
                   bbox=(-112, 36, -106, 41),
                   size=(300,250),
                   format='image/gif',
                   srs='EPSG:4326',)
        out = open(img_filename, 'wb')
        out.write(img.read())
        out.close()

        out_filename = self.mktempfile(suffix='.gif')
        try:
            cmd = ["gifsicle", operator]
            cmd.append("--delay=10")
            cmd.append("--append")
            cmd.append(img_filename)
            cmd.append("--output")
            cmd.append(out_filename)
            self.cmd(cmd=cmd, stdout=True)
        except:
            self.message(msg='gifscicle failed', force=True)

        self.status.set(msg="done", percentDone=90, propagate=True)

        self.output.setValue( out_filename )

        

        
