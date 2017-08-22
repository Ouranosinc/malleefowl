import os
import re
import copy
import netCDF4
from shutil import move
from malleefowl import config

import logging
LOGGER = logging.getLogger("PYWPS")


def resolve(location, f, defaults=None):
    place_holders = re.findall('\{(.*?)\}', location)

    if defaults:
        facet_values = copy.deepcopy(defaults)
    else:
        facet_values = {}

    try:
        nc = netCDF4.Dataset(f, 'a')
    except:
        return location.format(**facet_values)

    time_placeholders = ['initial_year', 'initial_month', 'initial_day',
                         'final_year', 'final_month', 'final_day']
    n = len(list(set(time_placeholders) - set(place_holders)))
    # Only fetch time components if they are place_holders
    if ('time' in nc.variables) and (n != len(time_placeholders)):
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
        elif facet in defaults:
            nc.setncattr(facet, facet_values[facet])
        elif facet not in facet_values:
            raise KeyError(facet)

    nc.close()
    return location.format(**facet_values)


def persist_files(files, location, defaults=None):
    persit_path = config.persist_root()
    thredds_url = config.thredds_url()

    p_files = []
    for f in files:
        # Remove the file:// part if present
        f = f.split('file://')[-1]
        expand_location = resolve(location, f, defaults)
        dst = os.path.join(persit_path, expand_location, os.path.basename(f))
        try:
            os.makedirs(os.path.dirname(dst))
        except OSError:
            pass
        move(f, dst)

        p_files.append('/'.join(s.strip('/') for s in [thredds_url, expand_location, os.path.basename(f)]))

    return p_files
