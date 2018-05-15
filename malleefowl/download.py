"""
TODO: handle parallel downloads
"""

import os
import requests
import urlparse
import threading
from Queue import Queue, Empty
import subprocess
from threddsclient import download_urls
from threddsclient import opendap_urls

from malleefowl import config
from malleefowl.utils import esgf_archive_path
from malleefowl.utils import flatten_auth_cookie
from malleefowl.exceptions import ProcessFailed

import logging
LOGGER = logging.getLogger("PYWPS")


def download_with_archive(url, credentials=None, cookie=None):
    """
    Downloads file. Checks before downloading if file is already in
    local esgf archive.
    """
    file_url = esgf_archive_path(url)
    if file_url is None:
        file_url = download(url, use_file_url=True, credentials=credentials, cookie=cookie)
    return file_url


def download(url, use_file_url=False, credentials=None, cookie=None):
    """
    Downloads url and returns local filename.

    :param url: url of file
    :param use_file_url: True if result should be a file url "file://", otherwise use system path.
    :param credentials: path to credentials if security is needed to download file
    :param cookie: key,value dict if security by cookie is needed to download file
    :returns: downloaded file with either file:// or system path
    """
    import urlparse
    parsed_url = urlparse.urlparse(url)
    if parsed_url.scheme == 'file':
        result = url
    else:
        result = wget(url=url, use_file_url=use_file_url, credentials=credentials, cookie=cookie)
    return result


def wget(url, use_file_url=False, credentials=None, cookie=None):
    """
    Downloads url and returns local filename.

    TODO: refactor cache handling.

    :param url: url of file
    :param use_file_url: True if result should be a file url "file://", otherwise use system path.
    :param credentials: path to credentials if security is needed to download file
    :param cookie: key,value dict if security by cookie is needed to download file
    :returns: downloaded file with either file:// or system path
    """
    LOGGER.info('downloading %s', url)

    parsed_url = urlparse.urlparse(url)
    filename = os.path.join(
        config.cache_path(),
        parsed_url.netloc,
        parsed_url.path.strip('/'))
    # check if in cache
    if os.path.isfile(filename):
        LOGGER.debug("using cached file.")
        if use_file_url:
            filename = "file://" + filename
        return filename

    local_cache_path = os.path.abspath(os.curdir)
    dn_filename = os.path.join(
        local_cache_path,
        parsed_url.netloc,
        parsed_url.path.strip('/'))
    if not os.path.isdir(os.path.dirname(dn_filename)):
        LOGGER.debug("Creating download directories.")
        os.makedirs(os.path.dirname(dn_filename), 0700)
    try:
        cmd = ["wget"]
        if credentials is not None:
            LOGGER.debug('using credentials')
            cmd.extend(["--certificate", credentials])
            cmd.extend(["--private-key", credentials])
            cmd.extend(["--ca-certificate", credentials])
        if cookie is not None:
            LOGGER.debug('using cookie')
            cmd.extend(["--header", "Cookie: {0}".format(flatten_auth_cookie(cookie))])
        cmd.append("--no-check-certificate")
        # if not LOGGER.isEnabledFor(logging.DEBUG):
        #    cmd.append("--quiet")
        cmd.append("--tries=3")                  # max 2 retries
        cmd.append("-N")                         # turn on timestamping
        cmd.append("--continue")                 # continue partial downloads
        #cmd.append("-x")                         # force creation of directories
        cmd.extend(["-O", dn_filename])
        cmd.extend(["-P", local_cache_path])  # directory prefix
        cmd.append(url)                          # download url
        LOGGER.debug("cmd: %s", ' '.join(cmd))
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        LOGGER.debug("output: %s", output)
    except subprocess.CalledProcessError as e:
        msg = "wget failed on {0}: {1.output}".format(url, e)
        LOGGER.exception(msg)
        raise ProcessFailed(msg)
    except Exception:
        msg = "wget failed on {0}.".format(url)
        LOGGER.exception(msg)
        raise ProcessFailed(msg)


    if not os.path.exists(filename):
        LOGGER.debug("linking downloaded file to cache.")
        if not os.path.isdir(os.path.dirname(filename)):
            LOGGER.debug("Creating cache directories.")
            os.makedirs(os.path.dirname(filename), 0700)
        try:
            os.link(dn_filename, filename)
        except Exception:
            LOGGER.warn('Could not link file, try to copy it ...')
            from shutil import copy2
            copy2(dn_filename, filename)
    if use_file_url:
        filename = "file://" + filename
    return filename


def download_files(urls=[], credentials=None, cookie=None, monitor=None):
    dm = DownloadManager(monitor)
    return dm.download(urls, credentials, cookie)


def download_files_from_thredds(url, recursive=False, cookie=None, monitor=None):
    urls = get_thredds_download_urls(url, cookie)
    return download_files(urls=urls, cookie=cookie, monitor=monitor)


def get_thredds_download_urls(url, cookie=None):
    return get_thredds_urls(download_urls, url, cookie)


def get_thredds_opendap_urls(url, cookie=None):
    return get_thredds_urls(opendap_urls, url, cookie)


def get_thredds_urls(thredds_fct, url, cookie=None):
    headers = dict(Cookie=flatten_auth_cookie(cookie)) if cookie else None

    try:
        urls = thredds_fct(url, headers=headers)
    except ValueError:
        # threddsclient don't check ret code before parsing the response and thus the real error message could be lost
        # Here we try a second time to check the return code and possibly get a better error message
        # If the return code is 200 we reraise the original parsing error
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise response.raise_for_status()
        raise
    return urls


class DownloadManager(object):
    def __init__(self, monitor=None):
        self.files = []
        self.count = 0
        self.monitor = monitor

    def show_status(self, message, progress):
        if self.monitor is None:
            LOGGER.info("%s, progress=%d/100", message, progress)
        else:
            self.monitor(message, progress)

    # The threader thread pulls an worker from the queue and processes it
    def threader(self):
        queue_full = True
        while queue_full:
            # Run the example job with the avail worker in queue (thread)
            # TODO: handle exception ... maybe download job should stop.
            try:
                # gets an worker from the queue
                worker = self.job_queue.get()
                self.download_job(**worker)
            except Empty:
                queue_full = False
            except Exception:
                LOGGER.exception('download failed!')
                queue_full = False
            finally:
                # completed with the job
                self.job_queue.task_done()

    def download_job(self, url, credentials, cookie):
        file_url = download_with_archive(url, credentials, cookie)
        with self.result_lock:
            self.files.append(file_url)
            self.count = self.count + 1
        progress = self.count * 100.0 / self.max_count
        self.show_status('Downloaded %d/%d' % (self.count, self.max_count),
                         progress)

    def download(self, urls, credentials=None, cookie=None):
        # start ...
        from datetime import datetime
        t0 = datetime.now()
        self.show_status("start downloading of %d files" % len(urls), 0)
        # lock for parallel search
        self.result_lock = threading.Lock()
        self.files = []
        self.count = 0
        self.max_count = len(urls)
        # init threading
        self.job_queue = Queue()
        # using max 4 thredds
        num_threads = min(4, len(urls))
        LOGGER.info('starting %d download threads', num_threads)
        for x in range(num_threads):
            t = threading.Thread(target=self.threader)
            # classifying as a daemon, so they will die when the main dies
            t.daemon = True
            # begins, must come after daemon definition
            t.start()
        for url in urls:
            # fill job queue
            self.job_queue.put(dict(url=url, credentials=credentials, cookie=cookie))

        # wait until the thread terminates.
        self.job_queue.join()
        # how long?
        duration = (datetime.now() - t0).seconds
        self.show_status(
            "downloaded %d files in %d seconds" % (len(urls), duration), 100)
        if len(self.files) != len(urls):
            raise ProcessFailed(
                "could not download all files %d/%d" %
                (len(self.files), len(urls)))
        # done
        return self.files
