import glob
import os

import dask
import dask.distributed
import dask_jobqueue
import xarray as xr
from dask.diagnostics import ProgressBar

# === SLURM cluster setup ===
cluster = dask_jobqueue.SLURMCluster(
    cores=4,
    memory="16GB",
    processes=1,
    queue="base",
    walltime="12:00:00",
    interface="ib0",
    local_directory="$TMPDIR",
    log_directory="/gxfs_work/geomar/smomw452/GLORYS12/revisions/slurm/",
)
client = dask.distributed.Client(cluster)
cluster.scale(jobs=15)

# === Paths ===
level0_path = "/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/data/level0_uniform_release_clim_land_nans/"
outpath = "/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/data/"
level1_path = os.path.join(outpath, "level1_uniform_release_clim_land_nans/")
os.makedirs(level1_path, exist_ok=True)

# === Step 2: Combine all level1 files into one zarr ===
level1_files = sorted(glob.glob(level1_path + "parcels_releases_seed-2345_*.zarr"))
print(f"{len(level1_files)} level1 files found", flush=True)

# === Trajectory index counter ===
global trj_idx
trj_idx = 0


def preprocess(ds):
    global trj_idx
    trj_number = int(ds["trajectory"].max().values) + 1
    ds["trajectory"] = ds["trajectory"] + trj_idx
    trj_idx = trj_idx + trj_number
    return ds


print("Building dask graph...", flush=True)
chunks = {"trajectory": 2000, "obs": -1}
ds = xr.open_mfdataset(
    level1_files,
    engine="zarr",
    preprocess=preprocess,
    combine="nested",
    concat_dim="trajectory",
    chunks=chunks,
)
print(f"Dataset built: {ds}", flush=True)

# Reassign trajectory indices to be 0, 1, 2, 3... chronologically
print("Reassigning trajectory indices...", flush=True)
ds["trajectory"] = xr.DataArray(range(ds.trajectory.size), dims="trajectory")

# Clear any leftover encoding from level1 files
for var in ds.data_vars:
    ds[var].encoding.pop("chunks", None)
    ds[var].encoding.pop("preferred_chunks", None)
ds["time"].encoding.pop("chunks", None)
ds["time"].encoding.pop("preferred_chunks", None)


# === Step 3: Save combined dataset to level2 ===
level2_path = os.path.join(outpath, "level2_uniform_release_clim_land_nans/")
os.makedirs(level2_path, exist_ok=True)
output_file = os.path.join(
    level2_path, "uniform_parcels_releases_seed-2345_2000-2020.zarr"
)

# Rechunk to uniform size before saving
print("Rechunking to uniform size...", flush=True)
ds = ds.chunk({"trajectory": 2000, "obs": -1})

print("Starting to save level2...", flush=True)
with ProgressBar():
    ds.to_zarr(output_file, mode="w", consolidated=True, safe_chunks=False)
print(f"Saved to {output_file}", flush=True)
