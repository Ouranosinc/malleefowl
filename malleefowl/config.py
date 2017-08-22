import os
import tempfile
from pywps import configuration

import logging
LOGGER = logging.getLogger(__name__)


def wps_url():
    return configuration.get_config_value("server", "url")


def thredds_url():
    return configuration.get_config_value("extra", "thredds_url")


def cache_path():
    mypath = configuration.get_config_value("cache", "cache_path")
    if not os.path.isdir(mypath):
        mypath = tempfile.mkdtemp(prefix='cache')
    LOGGER.debug("using cache %s", mypath)
    return mypath


def persist_root():
    mypath = configuration.get_config_value("extra", "persist_path")
    LOGGER.debug("using persist path %s", mypath)
    return mypath


def archive_root():
    value = configuration.get_config_value("extra", "archive_root")
    if value:
        path_list = [path.strip() for path in value.split(':')]
        LOGGER.debug("using archive root %s", path_list)
    else:
        path_list = []
    return path_list
