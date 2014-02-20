import json

class MyEncoder(json.JSONEncoder):
    
    def default(self, obj):
        #print 'default(', repr(obj), ')'
        # Convert objects to a dictionary of their representation
        d = {}
        try:
            d.update(obj.__dict__)
        except:
            pass
        return d

from owslib.wps import WebProcessingService, monitorExecution

def get_caps(service, json_format=False, verbose=False):
    wps = WebProcessingService(service, verbose=verbose)
    wps.getcapabilities()

    if json_format:
         # TODO: return result as yaml or json
         print MyEncoder(sort_keys=True, indent=2).encode(wps.processes)
    else:
        count = 0
        for process in wps.processes:
            count = count + 1
            print "%3d. %s [%s]" % (count, process.title, process.identifier)

def describe_process(service, identifier, json_format=False, verbose=False):
    wps = WebProcessingService(service, verbose=verbose)
    process = wps.describeprocess(identifier)

    if json_format:
        # TODO: return result as yaml or json
        print MyEncoder(sort_keys=True, indent=2).encode(process)
    else:
        print "Title            = ", process.title
        print "Identifier       = ", process.identifier
        print "Abstract         = ", process.abstract
        print "Store Supported  = ", process.storeSupported
        print "Status Supported = ", process.statusSupported
        print "Data Inputs      = ", reduce(lambda x,y: x + ', ' + y.identifier, process.dataInputs, '')
        print "Process Outputs  = ", reduce(lambda x,y: x + ', ' + y.identifier, process.processOutputs, '')

def execute(service, identifier, inputs=[], outputs=[], openid=None, password=None, file_identifiers=None, verbose=False):
    wps = WebProcessingService(service, verbose=verbose)
    if openid != None:
        inputs.append( ("openid", openid) )
    if password != None:
        inputs.append( ("password", password) )
    if file_identifiers != None:
        for file_identifier in file_identifiers.split(','):
            inputs.append( ("file_identifier", file_identifier) )
    execution = wps.execute(identifier, inputs=inputs, output=outputs)
    monitorExecution(execution, sleepSecs=1)
    status = execution.status
    print status
    print execution.processOutputs
    for out in execution.processOutputs:
        print out.reference
    #output = execution.processOutputs[0].reference

def main():
    import optparse

    parser = optparse.OptionParser(usage='%prog execute|describe|caps [options]',
                                   version='0.1')
    parser.add_option('-v', '--verbose',
                      dest="verbose",
                      default=False,
                      action="store_true",
                      help="verbose mode"
                      )
    parser.add_option('-s', '--service',
                      dest="service",
                      default="http://localhost:8090/wps",
                      action="store",
                      help="WPS service url (default: http://localhost:8090/wps)")
    parser.add_option('-i', '--identifier',
                      dest="identifier",
                      action="store",
                      help="WPS process identifier")
    parser.add_option('--json',
                      dest="json_format",
                      default=False,
                      action="store_true",
                      help="print results in json format")

    execute_opts = optparse.OptionGroup(
        parser, 'Execute Options',
        'Options for exection command')
    execute_opts.add_option('--input',
                            dest="inputs",
                            action="append",
                            default=[],
                            help="zero or more input params: key=value")
    execute_opts.add_option('--output',
                            dest="outputs",
                            action="append",
                            default=[],
                            help="one or more output params")
    parser.add_option_group(execute_opts)

    options, remainder = parser.parse_args()
    
    if options.verbose:
        print "SERVICE    = ", options.service
        print "IDENTIFIER = ", options.identifier
        print "INPUTS     = ", options.inputs
        print "OUTPUTS    = ", options.outputs
        print "JSON       = ", options.json
        print "COMMAND    = ", remainder

    inputs = []
    for param in options.inputs:
        key,value = param.split('=')
        inputs.append( (key, value) )

    outputs = []
    for param in options.outputs:
        outputs.append( (param, True) )

    if 'caps' in remainder:
        get_caps(
            service = options.service,
            json_format = options.json_format,
            verbose = options.verbose)
    elif 'info' in remainder:
        describe_process(
            service = options.service,
            identifier = options.identifier,
            json_format = options.json_format,
            verbose = options.verbose)
    elif 'run' in remainder:
        execute(
            service = options.service,
            identifier = options.identifier,
            inputs = inputs,
            outputs = outputs,
            verbose = options.verbose)
    else:
        print "Unknown command", remainder
        exit(1)

    
