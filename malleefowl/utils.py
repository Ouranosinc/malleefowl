"""
Utility functions for WPS processes.
"""

import json
from netCDF4 import Dataset
import os
import copy

from malleefowl import config

import logging
logger = logging.getLogger(__name__)


def esgf_archive_path(url):
    from os.path import join, isfile

    archive_path = None

    if 'thredds/fileServer/' in url:
        url_path = url.split('thredds/fileServer/')[1]
        logger.debug('check thredds archive: url_path=%s', url_path)
        # TODO: workaround for dkrz archive path
        rel_path = '/'.join(url_path.split('/')[1:])
        for root_path in config.archive_root():
            file_path = join(root_path, rel_path)
            logger.debug('file_path = %s', file_path)
            if isfile(file_path):
                logger.info('found in archive: %s', url)
                archive_path = 'file://' + file_path
                break
        if archive_path is None:
            logger.info('not found in archive: %s', url)
    return archive_path


def dupname(path, filename):
    """
    avoid dupliate filenames
    TODO: needs to be improved
    """
    newname = filename
    count = sum(1 for fname in os.listdir(path) if filename in fname)
    if count > 0:
        return newname + '_' + str(count)
    return newname


def user_id(openid):
    """generate user_id from openid"""

    import re

    ESGF_OPENID_REXP = r'https://(.*)/esgf-idp/openid/(.*)'

    user_id = None
    mo = re.match(ESGF_OPENID_REXP, openid)
    try:
        hostname = mo.group(1)
        username = mo.group(2)
        user_id = "%s_%s" % (username, hostname)
    except:
        raise Exception("unsupported openid")
    return user_id


def within_date_range(timesteps, start=None, end=None):
    from dateutil.parser import parse as date_parser
    start_date = None
    if start is not None:
        start_date = date_parser(start)
    end_date = None
    if end is not None:
        end_date = date_parser(end)
    new_timesteps = []
    for timestep in timesteps:
        candidate = date_parser(timestep)
        # within time range?
        if start_date is not None and candidate < start_date:
            continue
        if end_date is not None and candidate > end_date:
            break
        new_timesteps.append(timestep)
    return new_timesteps


def filter_timesteps(timesteps, aggregation="monthly", start=None, end=None):
    from dateutil.parser import parse as date_parser
    logger.debug("aggregation: %s", aggregation)

    if (timesteps is None or len(timesteps) == 0):
        return []
    timesteps.sort()
    work_timesteps = within_date_range(timesteps, start, end)

    new_timesteps = [work_timesteps[0]]

    for index in range(1, len(work_timesteps)):
        current = date_parser(new_timesteps[-1])
        candidate = date_parser(work_timesteps[index])

        if current.year < candidate.year:
            new_timesteps.append(work_timesteps[index])
        elif current.year == candidate.year:
            if aggregation == "daily":
                if current.timetuple()[7] == candidate.timetuple()[7]:
                    continue
            elif aggregation == "weekly":
                if current.isocalendar()[1] == candidate.isocalendar()[1]:
                    continue
            elif aggregation == "monthly":
                if current.month == candidate.month:
                    continue
            elif aggregation == "yearly":
                if current.year == candidate.year:
                    continue
            # all checks passed
            new_timesteps.append(work_timesteps[index])
        else:
            continue
    return new_timesteps


def nc_copy(source, target, overwrite=True, time_dimname='time', nchunk=10, istart=0, istop=-1, format='NETCDF3_64BIT'):
    """copy netcdf file from opendap to netcdf3 file

     :param overwrite:

          Overwite destination file (default is to raise an error if output file already exists).

     :param format:

          netcdf3 format to use (NETCDF3_64BIT by default, can be set to NETCDF3_CLASSIC)

     :param chunk:

          number of records along unlimited dimension to
          write at once. Default 10. Ignored if there is no unlimited
          dimension. chunk=0 means write all the data at once.

     :param istart:

          number of record to start at along unlimited dimension.
          Default 0.  Ignored if there is no unlimited dimension.

     :param istop:

          number of record to stop at along unlimited dimension.
          Default -1.  Ignored if there is no unlimited dimension.
    """

    nc_in = Dataset(source, 'r')
    nc_out = Dataset(target, 'w', clobber=overwrite, format=format)

    # create dimensions. Check for unlimited dim.
    unlimdimname = False
    unlimdim = None

    # create global attributes.
    logger.info('copying global attributes ...')
    nc_out.setncatts(nc_in.__dict__)
    logger.info('copying dimensions ...')
    for dimname, dim in nc_in.dimensions.items():
        if dim.isunlimited() or dimname == time_dimname:
            unlimdimname = dimname
            unlimdim = dim
            if istop == -1:
                istop = len(unlimdim)
            nc_out.createDimension(dimname, istop - istart)
            logger.debug('unlimited dimension = %s, length = %d', unlimdimname, len(unlimdim))
        else:
            nc_out.createDimension(dimname, len(dim))

    # create variables.
    for varname, ncvar in nc_in.variables.items():
        logger.info('copying variable %s', varname)
        # is there an unlimited dimension?
        if unlimdimname and unlimdimname in ncvar.dimensions:
            hasunlimdim = True
        else:
            hasunlimdim = False
        if hasattr(ncvar, '_FillValue'):
            FillValue = ncvar._FillValue
        else:
            FillValue = None
        var = nc_out.createVariable(varname, ncvar.dtype, ncvar.dimensions, fill_value=FillValue)
        # fill variable attributes.
        attdict = ncvar.__dict__
        if '_FillValue' in attdict:
            del attdict['_FillValue']
        var.setncatts(attdict)
        if hasunlimdim:  # has an unlim dim, loop over unlim dim index.
            # range to copy
            if nchunk:
                start = istart
                stop = istop
                step = nchunk
                if step < 1:
                    step = 1
                for n in range(start, stop, step):
                    nmax = n + nchunk
                    if nmax > istop:
                        nmax = istop
                    logger.debug('copy chunk [%d:%d]', n, nmax)
                    try:
                        var[n - istart:nmax - istart] = ncvar[n:nmax]
                    except:
                        msg = "n=%d nmax=%d istart=%d istop=%d" % (n, nmax, istart, istop)
                        raise Exception(msg)
            else:
                var[0:istop - istart] = ncvar[:]
        else:  # no unlim dim or 1-d variable, just copy all data at once.
            var[:] = ncvar[:]
        nc_out.sync()  # flush data to disk
    # close files.
    nc_out.close()
    nc_in.close()


class auto_list:
    """
    Implement a list that auto expand when the index exceed the current size of the list
    """
    def __init__(self, list_inst, default_val=0):
        self.list = list_inst
        self.default_val = default_val

    def __setitem__(self, key, value):
        self.resize(key + 1, self.default_val)
        self.list[key] = value

    def __iter__(self):
        for val in self.list:
            yield val

    def __len__(self):
        return len(self.list)

    def resize(self, new_size, default_val=0):
        size = len(self.list)
        if new_size > size:
            self.list.extend([default_val for _ in range(size, new_size)])


class DataWrapper:
    """
    Data wrapper use to move data in the dispel4py graph including headers along with the data
    """

    # This is the header specifying the mapping index of a data when travelling inside a parallel group
    HEADERS_MAP_INDEX = 'map_index'

    def __init__(self, payload=None, headers=None):
        self.payload = payload
        self.headers = headers or {}


class Monitor:
    """
    Base class for task monitoring providing specific function to implement
    """
    def __init__(self):
        pass

    def __deepcopy__(self, memo):
        """
        The original monitor instance must be preserved even when the owner is deep copied
        """
        return copy.copy(self)

    def update_status(self, message, progress=None):
        """
        Update the PyWPS status
        :param message: New message
        :param progress: New progress
        :return: None
        """
        pass

    def raise_exception(self, exception):
        """
        To be called when aan exception occurs in a task
        Because tasks are run in processes this function will allow a monitor implementation to gather every exceptions
        that will not be catched otherwise
        :param exception: The exception
        """
        pass

    def save_task_result(self, task, result):
        """
        Allow to save the result of a task
        :param task: The task name
        :param result: It's result
        """
        pass
