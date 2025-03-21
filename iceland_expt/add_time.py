import warnings
# import matplotlib.pyplot as plt
from glob import glob

import dask
import dask.distributed
import dask_jobqueue
# import parcels
import pandas as pd
import tqdm
import xarray as xr

warnings.filterwarnings("ignore")

cluster = dask_jobqueue.SLURMCluster(
    # Dask worker size
    cores=4,
    memory="75GB",
    processes=1,  # Dask workers per job
    # SLURM job script things
    queue="base",
    walltime="10:00:00",
    # Dask worker network and temporary storage
    interface="ib0",
    local_directory="$TMPDIR",  # for spilling tmp data to disk
    log_directory="slurm/",
)

client = dask.distributed.Client(cluster)

cluster.adapt(minimum=1, maximum=15)
print(client)


# inpath = "/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/iceland_expt/data2/"
# level1_files = sorted(glob(inpath + "level1/parcels_releases_seed-123_*.zarr"))
# print(f"{len(level1_files)} found")
# global trj_idx
# trj_idx = 0

# frequency = "24H"

# entire_times = pd.date_range(start="1993-01-01", end="2020-05-31", freq=frequency)


# def preprocess(ds):
#     """
#     The trajecetories in each dataset start with 0, to make them compatible
#     we add the last trajectory index to each trajectory.
#     """
#     global trj_idx
#     trj_number = max(ds["trajectory"])
#     ds["trajectory"] = ds["trajectory"] + trj_idx
#     # Select the first 100 observations
#     ds = ds.isel(obs=slice(0, 100)) 
#     start_time = pd.to_datetime(ds["time"].isel(trajectory=0, obs=0).values).floor("H")
#     freq = frequency
#     time = pd.date_range(start=start_time, periods=len(ds["obs"]), freq=freq)
#     ds["obs"] = time
#     del ds["time"]
#     ds = ds.rename({"obs": "time"})
#     ds = ds.reindex(time=entire_times)
#     trj_idx = trj_idx + trj_number
#     return ds


# print("Let's get cracking")

# # Split level1_files into several sublists for parallel processing
# sublists = [level1_files[i : i + 50] for i in range(0, len(level1_files), 50)]

# for f, filelist in enumerate(tqdm.tqdm(sublists)):
#     dss = []
#     for file in tqdm.tqdm(filelist):
#         ds = xr.open_dataset(file, engine="zarr")
#         ds = preprocess(ds)
#         dss.append(ds)

#     ds_merged = xr.concat(dss, dim="trajectory")
#     ds_merged.to_zarr(
#         f"/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/iceland_expt/data2/level3-2/slice_with_time_parcels_releases_seed-123_{f:03d}.zarr",
#         mode="w",
#         consolidated=True,
#     )

ds = xr.open_mfdataset(
    '/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/iceland_expt/data2/level3-2/slice_with_time_parcels_releases_seed-123_0*.zarr',
    engine="zarr",
    combine="nested",
    concat_dim="trajectory",
)

print("testing...")

# Saving the rechunked output
for var in ds.data_vars:
    del ds[var].encoding["chunks"]
    del ds[var].encoding['preferred_chunks']

ds.chunk({'trajectory':500, 'time': 100}).to_zarr("/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/iceland_expt/data2/level3-2/1_slice_with_time_parcels_releases_seed-123.zarr", mode="w", consolidated=True)
# 'time': 601 