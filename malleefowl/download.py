import os

from malleefowl import config

from malleefowl import wpslogging as logging
logger = logging.getLogger(__name__)

def download_with_archive(url, credentials=None):
    from .utils import esgf_archive_path
    local_url = esgf_archive_path(url)
    if local_url is None:
        local_url = download(url, use_file_url=True, credentials=credentials)
    return local_url

def download(url, use_file_url=False, credentials=None):
    from os.path import basename
    resource_name = basename(url)
    logger.debug('downloading %s', url)

    from subprocess import check_output
    try:
        cmd = ["wget"]
        if credentials is not None:
            logger.debug('using credentials')
            cmd.append("--certificate")
            cmd.append(credentials) 
            cmd.append("--private-key")
            cmd.append(credentials)
        cmd.append("--no-check-certificate")
        if not logger.isEnabledFor(logging.DEBUG):
            cmd.append("--quiet")
        cmd.append("-N")
        cmd.append("-P")
        cmd.append(config.cache_path())
        cmd.append(url)
        check_output(cmd)
    except:
        msg = "wget failed on %s. Maybe not authorized? " % (resource_name)
        logger.exception(msg)
        raise Exception(msg)

    from os.path import join
    result = join(config.cache_path(), resource_name)
    if use_file_url == True:
        result = "file://" + result
    return result


