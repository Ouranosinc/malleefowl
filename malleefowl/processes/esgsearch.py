from malleefowl.process import WPSProcess
from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

class ESGSearch(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(self,
            identifier = "esgsearch",
            title = "ESGF Search",
            version = "0.1",
            abstract="Search ESGF datasets, files and aggreations.")

        self.url = self.addLiteralInput(
            identifier="url",
            title="URL",
            abstract="URL of ESGF Search Index",
            default='http://localhost:8081/esg-search',
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        self.distrib = self.addLiteralInput(
            identifier = "distrib",
            title = "Distributed",
            abstract = "Flag for distributed search.",
            default = True,
            minOccurs=1,
            maxOccurs=1,
            type=type(True)
            )

        self.query = self.addLiteralInput(
            identifier = "query",
            title = "Query",
            abstract = "Query search string",
            default = '',
            minOccurs=1,
            maxOccurs=1,
            type=type('')
            )

        self.output = self.addComplexOutput(
            identifier="output",
            title="Search Result",
            abstract="JSON document with search result",
            metadata=[],
            formats=[{"mimeType":"test/json"}],
            asReference=True,
            )

    def execute(self):
        self.show_status("Starting ...", 0)

        from pyesgf.search import SearchConnection
        conn = SearchConnection(self.url.getValue(),
                                distrib=self.distrib.getValue())

        ctx = conn.new_context(
            project='CMIP5',
            model='HadCM3',
            experiment='decadal2000',
            time_frequency='day',
            realm='ocean',
            ensemble='r1i2p1')
                
        self.show_status("Datasets found=%d" % ctx.hit_count, 10)

        datasets = []
        keys = ['id', 'number_of_files', 'number_of_aggregations', 'size']
        for ds in ctx.search():
            ds_dict = {}
            for key in keys:
                ds_dict[key] = ds.json.get(key)
            datasets.append(ds_dict)

        self.show_status("Writing output ...", 10)

        import json
        outfile = self.mktempfile(suffix='.json')
        with open(outfile, 'w') as fp:
            json.dump(obj=datasets, fp=fp, indent=4, sort_keys=True)
        self.output.setValue( outfile )

        self.show_status("Done.", 100)

        
