"""
Create parcel fieldset

- Loading model data
- Convert individual model data fields to one xarray dataset
- Create parcels fieldset from xarray dataset
- Write fieldset to disk for later usage with parcels.from_parcels()
"""

import warnings
from glob import glob

import tqdm as tqdm
import xarray as xr

warnings.filterwarnings("ignore")


def get_files(inpath, min_ind=0, max_ind=None):
    ufiles = sorted(glob(f"{inpath}/U/*.nc"))[min_ind:max_ind]
    vfiles = sorted(glob(f"{inpath}/V/*.nc"))[min_ind:max_ind]
    wfiles = sorted(glob(f"{inpath}/W/*.nc"))[min_ind:max_ind]
    sfiles = sorted(glob(f"{inpath}/S/*.nc"))[min_ind:max_ind]
    tfiles = sorted(glob(f"{inpath}/T/*.nc"))[min_ind:max_ind]
    return ufiles, vfiles, wfiles, sfiles, tfiles


def create_mapping(ufiles, vfiles, wfiles, sfiles, tfiles):
    """Create mapping of parcel and model dimensions and variables"""
    coords = xr.open_dataset("/gxfs_work/geomar/smomw452/GLORYS12/coords.nc")

    variables = {
        "U": "vozocrtx",
        "V": "vomecrty",
        "W": "vovecrtz",
        "S": "vosaline",
        "T": "votemper",
    }

    filenames = {
        "U": {
            "lon": wfiles[0],
            "lat": wfiles[0],
            "depth": wfiles[0],
            "data": ufiles,
        },  # must use same everywhere w,files. but w depth is 0, northeast corner of T grid is lid
        "V": {"lon": wfiles[0], "lat": wfiles[0], "depth": wfiles[0], "data": vfiles},
        "W": {"lon": wfiles[0], "lat": wfiles[0], "depth": wfiles[0], "data": wfiles},
        "S": {"lon": wfiles[0], "lat": wfiles[0], "depth": wfiles[0], "data": sfiles},
        "T": {"lon": wfiles[0], "lat": wfiles[0], "depth": wfiles[0], "data": tfiles},
    }

    c_grid_dimensions = {
        "lon": "nav_lon",
        "lat": "nav_lat",
        "depth": "depthw",
        "time": "time_counter",
    }

    dimensions = {
        "U": c_grid_dimensions,
        "V": c_grid_dimensions,
        "W": c_grid_dimensions,
        "S": c_grid_dimensions,
        "T": c_grid_dimensions,
    }

    return (
        coords,
        variables,
        filenames,
        dimensions,
    )
