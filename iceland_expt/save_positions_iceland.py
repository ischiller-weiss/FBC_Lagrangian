import xarray as xr
# import matplotlib.pyplot as plt
import datetime
from glob import glob
import dask, dask.distributed
import dask_jobqueue
import matplotlib.pyplot as plt
import datetime
import numpy as np
# import parcels
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
#from xhistogram.xarray import histogram as xhist
import seawater as gsw
import matplotlib.colors as mcolors
import datetime as dt 
import tqdm



cluster = dask_jobqueue.SLURMCluster(

    # Dask worker size
    cores=4, memory='16GB',
    processes=1, # Dask workers per job
    
    # SLURM job script things
    queue='base', walltime='08:00:00',
    
    # Dask worker network and temporary storage
    interface='ib0', local_directory='$TMPDIR',  # for spilling tmp data to disk
    log_directory='slurm/' 
)

client = dask.distributed.Client(cluster)

cluster.scale(jobs=15) #15
client


#ds = xr.open_dataset('data2/level3-2/sliced_with_time_parcels_releases_seed-123.zarr', engine='zarr', chunks='auto') 

ds = xr.open_dataset('data2/level2/retry_with_time_parcels_releases_seed-123.zarr',engine='zarr', chunks='auto') 

# ds = xr.open_mfdataset(
#     '/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/iceland_expt/data2/level3-2/slice_with_time_parcels_releases_seed-123_0*.zarr',
#     engine="zarr",
#     combine="nested",
#     concat_dim="trajectory",
# )

# # Saving the rechunked output
# for var in ds.data_vars:
#     del ds[var].encoding["chunks"]
#     del ds[var].encoding['preferred_chunks']

# # rechunk
# ds = ds.chunk({'trajectory':500, 'time': 600})


def filter_beached_particles(ds,dim=None):
    """
    Filter particles in the dataset based on salinity.

    Parameters:
    ds (xarray.Dataset): The dataset containing salinity data.

    Returns:
    xarray.Dataset: A filtered dataset including only particles that have not reached zero salinity.
    """
    # Identify particles that reach zero salinity
    particles_reach_zero_salinity = (ds.salt == 0).any(dim)

    # Identify particles that have not reached zero salinity (beached)
    particles_beached = ~particles_reach_zero_salinity

    # Filter the dataset to include only particles that have not reached zero salinity
    ds_particles_reach_zero_salinity_beached = ds.where(particles_beached.compute(), drop=True)

    return ds_particles_reach_zero_salinity_beached

ds = filter_beached_particles(ds,'time')

def compute_density(ds):
    ds_density = xr.apply_ufunc(gsw.eos80.pden,ds.salt,ds.temp,0,dask='parallelized',output_dtypes=[np.float64]) - 1000
    ds_add_dens = ds.assign(sigma0=ds_density)
    return ds_add_dens

ds = compute_density(ds)

hours_to_days=0.0416667
hours_to_months=0.00136986
hours_to_years=0.000114155
time_conversion = hours_to_days

ds = ds.where((ds.age*hours_to_months) < 1,drop=False)

# print("dropnans and slice through times")

# ds_select_only_weeks = []

# # Loop through each trajectory
# for i in tqdm.tqdm(range(len(ds.trajectory))):
#     # Select the first 100 time indices for the current trajectory
#     selected_data = ds.isel(trajectory=i).dropna(dim='time').isel(time=slice(0,100))
#     ds_select_only_weeks.append(selected_data)

# # Combine the selected data back into a single dataset
# ds_select_only_weeks = xr.concat(ds_select_only_weeks, dim='trajectory')


ds_sel = ds.sel(time=pd.date_range("1993-01-01","2019-12-31", freq="1D"), method="nearest")

print("Let's get cracking")

positions_w_var = {}

for i in tqdm.tqdm(ds_sel.time):
    positions_w_var[i.dt.strftime('%Y-%m-%d %H:%M:%S').item(0)] = {
        'lat': ds.lat.sel(time=i).values,
        'lon': ds.lon.sel(time=i).values,
        'z': ds.z.sel(time=i).values
    }


# save vars and positions

pd.DataFrame(positions_w_var).T.to_parquet('z_daily.parq')