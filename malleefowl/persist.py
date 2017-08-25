import os
import re
import copy
import netCDF4
import requests
from shutil import move
from malleefowl import config
from malleefowl.authz import AuthZ

import logging

LOGGER = logging.getLogger("PYWPS")


def resolve(location, f, defaults=None):
    # Get all keys to replace in the location
    place_holders = re.findall('\{(.*?)\}', location)

    if defaults:
        facet_values = copy.deepcopy(defaults)

        # Extend the keys with the default value dict (will be used to patch dataset metadata)
        place_holders.extend(defaults.keys())
    else:
        facet_values = {}

    try:
        nc = netCDF4.Dataset(f, 'a')
    except:
        return location.format(**facet_values)

    time_placeholders = ['initial_year', 'initial_month', 'initial_day',
                         'final_year', 'final_month', 'final_day']
    # Only fetch time components if they are place_holders
    if 'time' in nc.variables and len(set(time_placeholders).intersection(set(place_holders))):
        nctime = nc.variables['time']
        if 'calendar' in nctime.ncattrs():
            calendar = nctime.getncattr('calendar')
        else:
            calendar = 'gregorian'
        datetime_i = netCDF4.num2date(nctime[0], nctime.units, calendar)
        facet_values['initial_year'] = str(datetime_i.year)
        facet_values['initial_month'] = str(datetime_i.month).zfill(2)
        facet_values['initial_day'] = str(datetime_i.day).zfill(2)
        datetime_f = netCDF4.num2date(nctime[-1], nctime.units, calendar)
        facet_values['final_year'] = str(datetime_f.year)
        facet_values['final_month'] = str(datetime_f.month).zfill(2)
        facet_values['final_day'] = str(datetime_f.day).zfill(2)
        # Could also add support for hour, minute, second, etc.

    for facet in place_holders:
        if facet in nc.ncattrs():
            facet_values[facet] = nc.getncattr(facet)
        elif "{0}_id".format(facet) in nc.ncattrs():
            facet_values[facet] = nc.getncattr("{0}_id".format(facet))
        # Facets not found in dataset metadata are added here
        elif facet in defaults:
            nc.setncattr(facet, facet_values[facet])
        elif facet not in facet_values:
            raise KeyError(facet)

    nc.close()
    return location.format(**facet_values)


def persist_files(files, location, defaults, overwrite, headers):
    authz_srv = AuthZ()
    persist_path = config.persist_path().rstrip('/')
    thredds_url = config.thredds_url().strip('/')
    known_extensions = config.persist_known_extensions().split(',')
    p_files = []

    for f in files:
        # Remove the file:// part if present
        f = f.split('file://')[-1]

        # Replace every place_holders by their values (the dataset is also updated with missing facets)
        expand_location = resolve(location, f, defaults).strip('/')

        # Check permission to write to the final location
        if not authz_srv.is_auth(expand_location, headers):
            raise requests.HTTPError('403 Forbidden')

        # If a known extension is not included in the location, the original basename is used
        file_parts = [expand_location]
        if expand_location.split('.')[-1] not in known_extensions:
            file_parts.append(os.path.basename(f))

        dst = '/'.join([persist_path] + file_parts)

        try:
            os.makedirs(os.path.dirname(dst))
        except OSError:
            pass

        if os.path.isfile(dst) and not overwrite:
            raise IOError('File already exists')

        move(f, dst)

        p_files.append('/'.join([thredds_url] + file_parts))

    return p_files
