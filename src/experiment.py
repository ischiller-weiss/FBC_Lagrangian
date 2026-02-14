#!/gxfs_home/geomar/smomw452//miniconda3/envs/py3_std_maps_2023-11-20/bin/python
import argparse
import asyncio
import datetime
import gc
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
import xarray as xr
from dask.distributed import get_worker
from loguru import logger
from parcels import Field

import create_fieldset as custom_fieldset
import kernel as custom_kernel
from haversine import interpolate_coordinates

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
    default="1995-01-01T00:00:00",
    help="Start date for particle release (format: YYYYMM-DDTHH:MM:SS)",
)
parser.add_argument(
    "--release_end",
    type=str,
    default="2002-12-31T00:00:00",
    help="End date for particle release (format: YYYYMM-DDTHH:MM:SS)",
)
parser.add_argument(
    "--frequency",
    type=str,
    default="5D",
    help="Frequency of particle release",
)
parser.add_argument(
    "--seeding",
    type=str,
    default="uniform",
    choices=["random", "uniform"],
    help="Seeding strategy for particle release",
)
parser.add_argument(
    "--output_dir",
    type=str,
    default="../data",
    help="Output directory for particle release files",
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

lon_bds = (-5.5, -3.5)
lat_bds = (61.05, 60.55)
start_depth = 600
end_depth = 1150

if args.seeding == "random":
    lon = np.random.uniform(*lon_bds, size=(n_particles_per_release,))
    lat = np.random.uniform(*lat_bds, size=(n_particles_per_release,))
    depth = np.random.uniform(start_depth, end_depth, size=(n_particles_per_release,))
elif args.seeding == "uniform":
    along_cross_section_points = 245
    number_of_depth_levels = 111
    depth_levels = np.linspace(start_depth, end_depth, number_of_depth_levels)
    depth = np.transpose(
        np.tile(depth_levels, (along_cross_section_points, 1))
    ).flatten()
    spacing_km, evenly_spaced_coords = interpolate_coordinates(
        [lat_bds[0], lon_bds[0]], [lat_bds[1], lon_bds[1]], along_cross_section_points
    )
    lat_pt = np.array([coord[0] for coord in evenly_spaced_coords])
    lon_pt = np.array([coord[1] for coord in evenly_spaced_coords])
    lat = np.tile(lat_pt, (number_of_depth_levels, 1)).flatten()
    lon = np.tile(lon_pt, (number_of_depth_levels, 1)).flatten()
    logger.info(f"Spacing between particles along cross-section: {spacing_km:.2f} km")
    logger.info(
        f"Vertical spacing between depth levels given {number_of_depth_levels} levels: {(end_depth - start_depth) / (number_of_depth_levels - 1):.2f} m"
    )
    logger.info(
        f"Total number of particles per release: {along_cross_section_points * number_of_depth_levels}"
    )

# Split particles into chunks of approximately 1000 particles each
chunk_size = 1000
n_total_particles = len(lon)
n_chunks = int(np.ceil(n_total_particles / chunk_size))

# Create chunks of particle positions
particle_chunks = []
for i in range(n_chunks):
    start_idx = i * chunk_size
    end_idx = min((i + 1) * chunk_size, n_total_particles)
    particle_chunks.append(
        {
            "lon": lon[start_idx:end_idx],
            "lat": lat[start_idx:end_idx],
            "depth": depth[start_idx:end_idx],
            "chunk_id": i,
            "n_particles": end_idx - start_idx,
        }
    )

logger.info(f"Total particles per release: {n_total_particles}")
logger.info(f"Split into {n_chunks} chunks of ~{chunk_size} particles each")

logger.info(f"Release times: {release_times}")

# Model filenames
inpath = "/gxfs_work/geomar/smomw452/GLORYS12/Data/"

max_ind = None
min_ind = None

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

# add in climatology

refs_json_path_T = "/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/data/climatology_refs_T.json"

ds_Tclim = xr.open_dataset(
    "reference://",
    engine="zarr",
    backend_kwargs={"consolidated": False},
    storage_options={"fo": refs_json_path_T},
    chunks="auto",
)

refs_json_path_S = "/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/data/climatology_refs_S.json"

ds_Sclim = xr.open_dataset(
    "reference://",
    engine="zarr",
    backend_kwargs={"consolidated": False},
    storage_options={"fo": refs_json_path_S},
    chunks="auto",
)

Tclim = ds_Tclim["votemper"]
Sclim = ds_Sclim["vosaline"]

ds_tfile = xr.open_dataset(tfiles[0])
nav_lat = ds_tfile["nav_lat"]
nav_lon = ds_tfile["nav_lon"]

Tclim = Tclim.assign_coords({"nav_lat": nav_lat, "nav_lon": nav_lon}).astype(np.float32)

Sclim = Sclim.assign_coords({"nav_lat": nav_lat, "nav_lon": nav_lon}).astype(np.float32)

with warnings.catch_warnings():
    warnings.simplefilter("ignore", parcels.FileWarning)
    fieldsetC = parcels.FieldSet.from_nemo(
        filenames,
        variables,
        dimensions,
        timestamps=timestamps,
        allow_time_extrapolation=True,
    )

fieldsetC.add_field(
    Field.from_xarray(
        Tclim,
        name="Tclim",
        dimensions={
            "lon": "nav_lon",
            "lat": "nav_lat",
            "depth": "deptht",
            "time": "time_counter",
        },
        allow_time_extrapolation=True,
    )
)

fieldsetC.add_field(
    Field.from_xarray(
        Sclim,
        name="Sclim",
        dimensions={
            "lon": "nav_lon",
            "lat": "nav_lat",
            "depth": "deptht",
            "time": "time_counter",
        },
        allow_time_extrapolation=True,
    )
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
    output_dir: str,
    chunk_id: int = 0,
):
    # Force garbage collection at the start to free up memory from previous tasks
    gc.collect()
    logging.info("Garbage collected at start of run_parcels")

    times = [t.to_pydatetime() for t in release_times]
    output_path = f'{output_dir}/parcels_releases_seed-{seed}_chunk-{chunk_id:03d}_{release_times[0].strftime("%Y%m%d%H")}-{release_times[-1].strftime("%Y%m%d%H")}.zarr'
    done_marker = output_path + ".done"

    # Skip if computation already completed successfully
    if os.path.exists(done_marker):
        logging.info(f"Computation already completed for {output_path}, skipping")
        return

    logging.info(f"Running parcels for release times: {times}")
    pset = parcels.ParticleSet.from_list(
        fieldset=fieldsetC,
        pclass=custom_kernel.SampleParticle,
        lon=np.tile(lon, len(release_times)),
        lat=np.tile(lat, len(release_times)),
        depth=np.tile(depth, len(release_times)),
        time=np.repeat(times, len(lon)),
    )

    logging.info(f"Created {len(pset)} particles")

    tries = 0
    while tries < 5:
        try:
            pset.execute(kernels, runtime=1)
            tries = np.inf
        except Exception as e:
            logging.error(f"Error in execution: {e}")
            logging.info("Retrying...")
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
    logging.info(land_indices)
    logging.info(f"Removed {count} particles initialized on land")

    # build composite kernel
    kernel = pset.Kernel(kernels)

    outputfile = parcels.ParticleFile(
        output_path,
        pset,
        timedelta(days=1),
        chunks=(len(pset), 31 * 365),
    )  # 31 years for max backtracking expt, 2024 - 1994, 365*output freq

    runtime = np.min(release_times) - datetime.datetime(1993, 1, 2)
    logging.info(f"Runtime: {runtime}")

    tries = 0
    while tries < 5:
        try:
            pset.execute(
                kernel,
                runtime=runtime,
                dt=-timedelta(minutes=10),
                output_file=outputfile,
            )
            tries = np.inf
        except Exception as e:
            logging.error(f"Error in execution: {e}")
            logging.info("Retrying...")
            tries += 1
            time.sleep(10)
            pass

    # Write completion marker file to indicate successful execution
    with open(done_marker, "w") as f:
        f.write(f"Completed at {datetime.datetime.now()}\n")

    # Force garbage collection before shutting down worker
    logging.info("Garbage collecting before worker shutdown")
    gc.collect()
    logging.info("Garbage collection completed")

    # Shut down the current worker to ensure next run gets a fresh one
    try:
        worker = get_worker()
        asyncio.run(worker.close(nanny=True, timeout=10))
    except Exception as e:
        logging.warning(f"Failed to shut down worker after completion: {e}")


if __name__ == "__main__":
    kernels = [
        parcels.AdvectionRK4_3D,
        custom_kernel.sampling,
        custom_kernel.SampleTSAnomaly,
        custom_kernel.age,
        custom_kernel.velocity_sampling,
        custom_kernel.TotalDistance,
        custom_kernel.DeleteParticle_outside_domain_beached,
    ]

    # Create a list of (release_time, chunk) tuples
    release_chunk_pairs = [
        (rt, chunk) for rt in release_times for chunk in particle_chunks
    ]

    logger.info(
        f"Total number of jobs: {len(release_chunk_pairs)} (release_times: {len(release_times)} × chunks: {len(particle_chunks)})"
    )

    runs = db.from_sequence(
        release_chunk_pairs, npartitions=len(release_chunk_pairs)
    ).map(
        lambda pair: run_parcels(
            [pair[0]],
            pair[1]["lon"],
            pair[1]["lat"],
            pair[1]["depth"],
            pair[1]["n_particles"],
            fieldsetC,
            kernels=kernels,
            seed=seed,
            output_dir=args.output_dir,
            chunk_id=pair[1]["chunk_id"],
        )
    )

    cluster = dask_jobqueue.SLURMCluster(
        # Dask worker size
        cores=1,
        processes=1,
        job_cpu=1,
        memory="20GB",
        # SLURM job script things
        queue="base",
        walltime="0-13:00:00",
        # Dask worker network and temporary storage
        interface="ib0",
        local_directory="$TMPDIR",  # for spilling tmp data to disk
        log_directory=f"../logs/{jobid}",
        job_extra_directives=[
            f"--error=../logs/{jobid}/dask-worker-{jobid}.%j.%N.%s.log",
            f"--output=../logs/{jobid}/dask-worker-{jobid}.%j.%N.%s.log",
            "--exclude=nesh-clk[352,356,358,363,377,384,387,390-391,394,396,398,414-416,428,433-434,438,440,445-446,454,456,459,469-470,479,483,493,502,511,515,529,538,555,557,570,573,586-587,594,598]",
        ],
        worker_extra_args=["--lifetime", "12h"],
    )

    client = dask.distributed.Client(cluster)
    logger.info(client)

    cluster.adapt(
        minimum=1,
        maximum=200,
    )

    # Submit tasks individually and handle failures without cancelling the full run
    delayed_runs = runs.to_delayed()
    futures = client.compute(delayed_runs, retries=2)
    for future in tqdm.tqdm(futures, total=len(futures)):
        try:
            future.result()
        except Exception as e:
            logger.error(f"Task failed after retries: {e}")
