import logging

@property
def DEBUG():
    return logging.DEBUG

class TraceLogger(object):
    def __init__(self, filename="trace.log"):
        self.logger = open(filename, "a")

    def write(self, message):
        import datetime 
        log_message = '{message}'.format(
            #asctime=str(datetime.datetime.now())[:19],
            message=message)
        self.logger.write(log_message + '\n')
        self.logger.flush()

def getLogger(name):
    from pywps import config
    log_path = config.getConfigValue("malleefowl", "logPath")
    log_level = config.getConfigValue("malleefowl", "logLevel")
    
    logger = logging.getLogger(name)
    if 'DEBUG' in log_level:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)-15s - %(name)-20s - %(levelname)-8s %(message)s')

    # log stdout to trace
    import os.path
    import sys
    sys.stdout = TraceLogger(os.path.join(log_path, 'malleefowl_trace.log'))
    
    # warn
    fh = logging.StreamHandler(stream=sys.stdout) #os.path.join(log_path, 'malleefowl_warn.log'))
    fh.setLevel(logging.WARN)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # info
    fh = logging.StreamHandler(stream=sys.stdout) #os.path.join(log_path, 'malleefowl_info.log'))
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # debug
    fh = logging.StreamHandler(stream=sys.stdout) #os.path.join(log_path, 'malleefowl_debug.log'))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger
    
