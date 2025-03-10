#!/gxfs_home/geomar/smomw452//miniconda3/envs/py3_std_maps_2023-11-20/bin/python
import argparse
import logging
import os
import subprocess
import time
import warnings
from datetime import timedelta

import dask
import dask.bag as db
import dask.distributed
import dask_jobqueue
import numpy as np
import pandas as pd
import parcels
import tqdm as tqdm
from loguru import logger

import create_fieldset as custom_fieldset
import kernel as custom_kernel

warnings.filterwarnings("ignore")


def get_slurm_jobid():
    jobid = os.getenv("SLURM_JOB_ID")
    if jobid is None:
        try:
            jobid = (
                subprocess.check_output(
                    ["squeue", "--noheader", "--format=%i", "-u", os.getenv("USER")]
                )
                .decode()
                .strip()
                .split("\n")[0]
            )
        except Exception as e:
            logging.error(f"Failed to get SLURM job ID: {e}")
            jobid = None
    return jobid


jobid = get_slurm_jobid()

# Argument parser
parser = argparse.ArgumentParser(description="Run particle tracking experiment.")
parser.add_argument(
    "--release_start",
    type=str,
    default="1993-01-01T00:00:00",
    help="Start date for particle release (format: YYYYMM-DDTHH:MM:SS)",
)
parser.add_argument(
    "--release_end",
    type=str,
    default="2019-12-31T00:00:00",
    help="End date for particle release (format: YYYYMM-DDTHH:MM:SS)",
)
parser.add_argument(
    "--frequency",
    type=str,
    default="5D",
    help="Frequency of particle release",
)
args = parser.parse_args()

logger.add(f"logs/{jobid}/experiment.log")

logger.info(f"SLURM Job ID: {jobid}")

release_times = pd.date_range(
    start=args.release_start, end=args.release_end, freq=args.frequency
)

# Set random seed
seed = 123
np.random.seed(seed)

# Settings

n_particles_per_release = 500

lon_bds = (-7, -11)
lat_bds = (65, 65)

lon = np.random.uniform(*lon_bds, size=(n_particles_per_release,))
lat = np.random.uniform(*lat_bds, size=(n_particles_per_release,))
depth = np.random.uniform(25, 35, size=(n_particles_per_release,))

logger.info(f"Release times: {release_times}")

# Model filenames
inpath = "/gxfs_work/geomar/smomw452/GLORYS12/Data/"

max_ind = None
min_ind = 2  # start from 1993!

ufiles, vfiles, wfiles, sfiles, tfiles = custom_fieldset.get_files(
    inpath, min_ind=min_ind, max_ind=max_ind
)
logger.info(f"Number of files: {len(ufiles)}")

coords, variables, filenames, dimensions = custom_fieldset.create_mapping(
    ufiles, vfiles, wfiles, sfiles, tfiles
)


timestamps = np.arange(
    np.datetime64(ufiles[0][-13:-3]),
    np.datetime64(ufiles[-1][-13:-3])
    + np.timedelta64(1, "h"),  # adding an hour to include the last day
    np.timedelta64(1, "D"),
)
assert (
    len(ufiles) == len(vfiles) == len(timestamps)
), "Different number of U, V files and timestamps"
timestamps = [[t] for t in timestamps]  # convert to a list of lists

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

with warnings.catch_warnings():
    warnings.simplefilter("ignore", parcels.FileWarning)
    fieldsetC = parcels.FieldSet.from_nemo(
        filenames,
        variables,
        dimensions,
        timestamps=timestamps,
        allow_time_extrapolation=True,
    )

# Prepare particle release
lon_release = lon  # longitude of release
lat_release = lat  # latitude of release
depth_release = depth  # depth of release, meters
time_release = release_times


# Parallel run
def run_parcels(
    release_times: list,
    lon: np.array,
    lat: np.array,
    depth: np.array,
    n_particles_per_release: int,
    fieldsetC: parcels.FieldSet,
    kernels: list,
    seed: int,
):
    times = [t.to_pydatetime() for t in release_times]
    print(f"Running parcels for release times: {times}")
    pset = parcels.ParticleSet.from_list(
        fieldset=fieldsetC,
        pclass=custom_kernel.SampleParticle,
        lon=np.tile(lon, len(release_times)),
        lat=np.tile(lat, len(release_times)),
        depth=np.tile(depth, len(release_times)),
        time=np.repeat(times, len(lon_release)),
    )

    print(f"Created {len(pset)} particles")

    tries = 0
    while tries < 5:
        try:
            pset.execute(kernels, runtime=1)
            tries = np.inf
        except Exception as e:
            print(f"Error in execution: {e}")
            print("Retrying...")
            tries += 1
            time.sleep(10)
            pass

    # Get land_indices of current release
    t = np.zeros(len(pset))
    ## detect via temperature land particles
    for i, p in enumerate(pset):
        t[i] = p.temp
    land_indices = np.argwhere(t == 0).flatten()
    pset.remove_indices(land_indices)
    count = len(land_indices)
    print(land_indices)
    print(f"Removed {count} particles initialized on land")

    # build composite kernel
    kernel = pset.Kernel(kernels)

    outputfile = parcels.ParticleFile(
        f'data/parcels_releases_seed-{seed}_{release_times[0].strftime("%Y%m%d%H")}-{release_times[-1].strftime("%Y%m%d%H")}.zarr',
        pset,
        timedelta(hours=24),
        chunks=(len(pset), 1),
    )

    runtime = timedelta(days=600)
    print(f"Runtime: {runtime}")

    tries = 0
    while tries < 5:
        try:
            pset.execute(
                kernel,
                runtime=runtime,
                dt=timedelta(minutes=10),
                output_file=outputfile,
            )
            tries = np.inf
        except Exception as e:
            print(f"Error in execution: {e}")
            print("Retrying...")
            tries += 1
            time.sleep(10)
            pass


cluster = dask_jobqueue.SLURMCluster(
    # Dask worker size
    cores=1,
    processes=1,
    job_cpu=1,
    n_workers=1,
    memory="32GB",
    # SLURM job script things
    queue="base",
    walltime="1-12:00:00",
    # Dask worker network and temporary storage
    interface="ib0",
    local_directory="$TMPDIR",  # for spilling tmp data to disk
    log_directory=f"logs/{jobid}",
    worker_extra_args=["--lifetime", "34h", "--lifetime-stagger", "4m"]
)

client = dask.distributed.Client(cluster)
logger.info(client)

cluster.adapt(
    minimum=1,
    maximum=100,
)

kernels = [
    parcels.AdvectionRK4_3D,
    custom_kernel.sampling,
    custom_kernel.age,
    custom_kernel.velocity_sampling,
    custom_kernel.DeleteParticle_outside_domain_beached,
]
runs = db.from_sequence(release_times, npartitions=len(release_times)).map(
    lambda t: run_parcels(
        [t],
        lon,
        lat,
        depth,
        n_particles_per_release,
        fieldsetC,
        kernels=kernels,
        seed=seed,
    )
)

runs.compute()
