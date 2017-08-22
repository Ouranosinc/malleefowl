import os
import re
import netCDF4
from shutil import move
from malleefowl import config

import logging
LOGGER = logging.getLogger("PYWPS")


def resolve(location, place_holders, f):
    try:
        nc = netCDF4.Dataset(f, 'r')
    except:
        nc = None

    facet_values = {}
    for facet in place_holders:
        if hasattr(nc, facet):
            value = getattr(nc, facet)
        elif hasattr(nc, facet + '_id'):
            value = getattr(nc, facet + '_id')
        else:
            value = 'unknown'
        facet_values[facet] = value
    return location.format(**facet_values)


def persist_files(files, location):
    persit_path = config.persist_root()
    thredds_url = config.thredds_url()

    place_holders = re.findall('\{(.*?)\}', location)

    p_files = []
    for f in files:
        # Remove the file:// part if present
        f = f.split('file://')[-1]
        if place_holders:
            expand_location = resolve(location, place_holders, f)
        else:
            expand_location = location
        dst = os.path.join(persit_path, expand_location, os.path.basename(f))
        try:
            os.makedirs(os.path.dirname(dst))
        except OSError:
            pass
        move(f, dst)

        p_files.append('/'.join(s.strip('/') for s in [thredds_url, expand_location, os.path.basename(f)]))

    return p_files
