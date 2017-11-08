import os
import tempfile
from pywps import configuration

import logging
LOGGER = logging.getLogger("PYWPS")

DEFAULT_NODE = 'default'
DKRZ_NODE = 'dkrz'
IPSL_NODE = 'ipsl'


def wps_url():
    return configuration.get_config_value("server", "url")


def thredds_url():
    return configuration.get_config_value("extra", "thredds_url")


def authz_url():
    return configuration.get_config_value("extra", "authz_url")

def thredds_service_name():
    return configuration.get_config_value("extra", "authz_thredds_service_name")

def cache_path():
    cache_path = configuration.get_config_value("cache", "cache_path")
    if not cache_path:
        LOGGER.warn("No cache path configured. Using default value.")
        cache_path = os.path.join(configuration.get_config_value("server", "outputpath"), "cache")
    return cache_path


def persist_path():
    mypath = configuration.get_config_value("extra", "persist_path")
    LOGGER.debug("using persist path %s", mypath)
    return mypath


def persist_known_extensions():
    return configuration.get_config_value("extra", "known_extensions")


def archive_root():
    value = configuration.get_config_value("extra", "archive_root")
    if value:
        path_list = [path.strip() for path in value.split(':')]
        LOGGER.debug("using archive root %s", path_list)
    else:
        path_list = []
    return path_list


def archive_node():
    node = configuration.get_config_value("extra", "archive_node")
    node = node or 'default'
    node = node.lower()
    return node


def viz_mapping(protocol):
    if protocol in ['wms', 'opendap']:
        value = configuration.get_config_value("extra", '{0}_mapping'.format(protocol))
        if value:
            refs = value.split(',')
            src_refs = refs[::2]
            viz_refs = refs[1::2]
            return zip(src_refs, viz_refs)
    return []

