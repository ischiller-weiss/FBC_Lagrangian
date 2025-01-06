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
    default="2003-01-01T00:00:00",
    help="Start date for particle release (format: YYYYMM-DDTHH:MM:SS)",
)
parser.add_argument(
    "--release_end",
    type=str,
    default="2020-06-03T00:00:00",
    help="End date for particle release (format: YYYYMM-DDTHH:MM:SS)",
)
parser.add_argument(
    "--frequency",
    type=str,
    default="5D",
    help="Frequency of particle release",
)
args = parser.parse_args()

logger.add(f"../logs/{jobid}/experiment.log")

logger.info(f"SLURM Job ID: {jobid}")

release_times = pd.date_range(
    start=args.release_start, end=args.release_end, freq=args.frequency
)

# Set random seed
seed = 2345
np.random.seed(seed)

# Settings

n_particles_per_release = 1_000

lon_bds = (-6.5, -2.5)
lat_bds = (61.3, 60.3)

lon = np.random.uniform(*lon_bds, size=(n_particles_per_release,))
lat = np.random.uniform(*lat_bds, size=(n_particles_per_release,))
depth = np.random.uniform(650, 1100, size=(n_particles_per_release,))

logger.info(f"Release times: {release_times}")

# Model filenames
inpath = "/gxfs_work/geomar/smomw452/GLORYS12/"

max_ind = None
min_ind = 2  # start from 1993!

ufiles, vfiles, wfiles, sfiles, tfiles = custom_fieldset.get_files(
    inpath, min_ind=min_ind, max_ind=max_ind
)
logger.info(f"Number of files: {len(ufiles)}")

coords, variables, filenames, dimensions = custom_fieldset.create_mapping(
    ufiles, vfiles, wfiles, sfiles, tfiles
)

if not os.path.exists("../../fieldsetC_U.nc"):
    # Set up dask cluster
    cluster = dask_jobqueue.SLURMCluster(
        # Dask worker size
        cores=4,
        memory="16GB",
        processes=1,  # Dask workers per job
        # SLURM job script things
        queue="base",
        walltime="04:00:00",
        # Dask worker network and temporary storage
        interface="ib0",
        local_directory="$TMPDIR",  # for spilling tmp data to disk
        log_directory="slurm/",
    )

    client = dask.distributed.Client(cluster)
    cluster.adapt(minimum=5, maximum=5)

    print(client)
    print(
        "To connect to the dask dashboard you might need to do port forwarding to the exact same node this script is running on, e.g. `ssh -L 8787:localhost:8787 this.node.com`"
    )

    ds = custom_fieldset.create_dataset(ufiles, vfiles, wfiles, sfiles, tfiles)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", parcels.FileWarning)
        fieldsetC = parcels.FieldSet.from_nemo(
            ds, variables, dimensions, allow_time_extrapolation=False
        )

    fieldsetC.write("../../fieldsetC_")
else:
    variables = {"U": "vozocrtx", "V": "vomecrty", "W": "W", "S": "S", "T": "T"}
    fieldsetC = parcels.FieldSet.from_parcels(
        "../../fieldsetC_", extra_fields=variables
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
        f'../data/parcels_releases_seed-{seed}_{release_times[0].strftime("%Y%m%d%H")}-{release_times[-1].strftime("%Y%m%d%H")}.zarr',
        pset,
        timedelta(hours=12),
        chunks=(500 * 27 * 2, 365),
    )  # timedelta was 6 before

    tries = 0
    while tries < 5:
        try:
            pset.execute(
                kernel,
                runtime=timedelta(days=365 * 27),
                dt=-timedelta(minutes=10),
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
    cores=32,
    n_workers=32,
    memory="180GB",
    # SLURM job script things
    queue="base",
    walltime="1-12:00:00",
    # Dask worker network and temporary storage
    interface="ib0",
    local_directory="$TMPDIR",  # for spilling tmp data to disk
    log_directory=f"../logs/{jobid}",
    worker_extra_args=["--lifetime", "34h", "--lifetime-stagger", "4m"],
)

client = dask.distributed.Client(cluster)
logger.info(client)

cluster.adapt(
    minimum=1,
    maximum=20,
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
