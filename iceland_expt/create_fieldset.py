"""
Create parcel fieldset

- Loading model data
- Convert individual model data fields to one xarray dataset
- Create parcels fieldset from xarray dataset
- Write fieldset to disk for later usage with parcels.from_parcels()
"""

import logging
import warnings
from glob import glob

import cf
import numpy as np
import parcels
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


def create_dataset(ufiles, vfiles, wfiles, sfiles, tfiles):
    """Create xarray dataset from model data"""

    ## Get depthw from one of the files to set it for all variables
    ds = xr.open_dataset(wfiles[0])
    depthw = ds.depthw.values

    def get_time(ds):
        """
        Interpret time based on unit and save as integer since 1970-01-01
        """
        time_var = ds.time_counter
        time_units = time_var.units
        cf_time = cf.Data(time_var.values, units=time_units)
        time = cf_time.datetime_array
        time = np.array(time, dtype="datetime64[s]")
        time = (time - np.datetime64("1970-01-01")) / np.timedelta64(1, "s")
        return time

    def preprocessor(ds):
        time = get_time(ds)
        ds["time_counter"] = time
        return ds

    dss = []
    for var_file in tqdm.tqdm([ufiles, vfiles, wfiles, sfiles, tfiles]):
        ds_ = xr.open_mfdataset(
            var_file,
            combine="nested",
            concat_dim="time_counter",
            decode_cf=False,
            parallel=True,
            preprocess=preprocessor,
        )
        if "deptht" in ds_.coords:
            # TODO: check if u,v,t,s can really be set to depthw although they are on deptht
            ds_ = ds_.rename({"deptht": "depthw"})
            ds_["depthw"] = depthw
        dss.append(ds_)
    ds = xr.merge(dss, compat="override")
    ds = ds.set_coords(
        ["nav_lat", "nav_lon"]
    )  # nav_lat and nav_lon are not in the coords of the merged dataset but data_vars
    ds["nav_lat"] = ds.nav_lat.isel(time_counter=0)
    ds["nav_lon"] = ds.nav_lon.isel(time_counter=0)
    ds["time_counter"] = cf.Data(
        ds.time_counter.values, units="seconds since 1970-01-01"
    ).datetime_array
    ds["time_counter"] = ds.time_counter.astype("datetime64[s]")
    return ds


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


