# process_data.py
import glob
import os

import dask
import dask.distributed
import dask_jobqueue
import tqdm
import xarray as xr

# cluster = dask_jobqueue.SLURMCluster(
#     # Dask worker size
#     cores=4,
#     memory="16GB",
#     processes=1,  # Dask workers per job
#     # SLURM job script things
#     queue="base",
#     walltime="00:30:00",
#     # Dask worker network and temporary storage
#     interface="ib0",
#     local_directory="$TMPDIR",  # for spilling tmp data to disk
#     log_directory="slurm/",
# )

# cluster = dask.distributed.LocalCluster()

# client = dask.distributed.Client(cluster)
# client

cluster = dask_jobqueue.SLURMCluster(
    # Dask worker size
    cores=4,
    memory="16GB",
    processes=1,  # Dask workers per job
    # SLURM job script things
    queue="highmem",
    walltime="04:00:00",
    # Dask worker network and temporary storage
    interface="ib0",
    local_directory="$TMPDIR",  # for spilling tmp data to disk
    log_directory="slurm/",
)

client = dask.distributed.Client(cluster)

cluster.scale(jobs=4)  # 15
client

# def main():
#     # Initialize the Dask cluster
#     cluster = dask_jobqueue.SLURMCluster(
#         # Dask worker size
#         cores=4,
#         memory="16GB",
#         processes=1,  # Dask workers per job
#         # SLURM job script things
#         queue="base",
#         walltime="00:30:00",
#         # Dask worker network and temporary storage
#         interface="ib0",
#         local_directory="$TMPDIR",  # for spilling tmp data to disk
#         log_directory="slurm/",
#     )

#     # Create a Dask client
#     client = dask.distributed.Client(cluster)
#     print(client)  # Optionally print the client information

# if __name__ == '__main__':
#     main()


global trj_idx
trj_idx = 0


def preprocess(ds):
    """
    The trajecetories in each dataset start with 0, to make them compatible
    we add the last trajectory index to each trajectory.
    """
    global trj_idx
    trj_number = max(ds["trajectory"])
    ds["trajectory"] = ds["trajectory"] + trj_idx
    trj_idx = trj_idx + trj_number
    return ds


# Rechunking the parcels output


inpath = "/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/data/"
parcel_output_files = sorted(glob.glob(inpath + "parcels_releases_seed-2345_*.zarr"))
print(f"{len(parcel_output_files)} found")
chunks = {"trajectory": 391, "obs": -1}
output_path_format = os.path.join(inpath, "level1/{file}")
if not os.path.exists(os.path.join(inpath, "level1")):
    os.makedirs(os.path.join(inpath, "level1"))
for file in tqdm.tqdm(parcel_output_files):
    ds = xr.open_zarr(file)
    for var in ds.data_vars:
        del ds[var].encoding["chunks"]
        del ds[var].encoding["preferred_chunks"]
    ds["time"] = ds.time.astype("float64")
    ds.chunk(chunks).to_zarr(
        output_path_format.format(file=os.path.basename(file)),
        mode="w",
        consolidated=True,
    )


# loading the rechunked output to make one chunked zarr file


level1_files = sorted(glob.glob(inpath + "level1/parcels_releases_seed-2345_*.zarr"))
print(f"{len(level1_files)} found")


ds = xr.open_mfdataset(
    level1_files,
    engine="zarr",
    preprocess=preprocess,
    combine="nested",
    concat_dim="trajectory",
)


# Saving the rechunked output


ds.to_zarr("data/level2/parcels_releases_seed-2345.zarr", mode="w", consolidated=True)
