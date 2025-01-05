#!/gxfs_home/geomar/smomw452//miniconda3/envs/py3_std_maps_2023-11-20/bin/python
import logging
import os
import warnings
from datetime import timedelta

import dask
import dask.distributed
import dask_jobqueue
import numpy as np
import pandas as pd
import parcels
import tqdm as tqdm

import create_fieldset as custom_fieldset
import kernel as custom_kernel

warnings.filterwarnings("ignore")

# Set random seed
np.random.seed(2345)

# Settings

n_particles_per_release = 10_000

lon_bds = (-6.5, -2.5)
lat_bds = (61.3, 60.3)

lon = np.random.uniform(*lon_bds, size=(n_particles_per_release,))
lat = np.random.uniform(*lat_bds, size=(n_particles_per_release,))
depth = np.random.uniform(650, 1100, size=(n_particles_per_release,))

release_times = pd.date_range(start="2003-01-01", end="2020-06-03", freq="5D", unit="s")

logging.info(f"Release times: {release_times}")

# Model filenames
inpath = "/gxfs_work/geomar/smomw452/GLORYS12/"

max_ind = None
min_ind = 2  # start from 1993!

ufiles, vfiles, wfiles, sfiles, tfiles = custom_fieldset.get_files(
    inpath, min_ind=min_ind, max_ind=max_ind
)
logging.info(f"Number of files: {len(ufiles)}")

coords, variables, filenames, dimensions = custom_fieldset.create_mapping(
    ufiles, vfiles, wfiles, sfiles, tfiles
)

if not os.path.exists("/gxfs_work/geomar/smomw452/GLORYS12/fieldsetC_U.nc"):
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

    fieldsetC.write("/gxfs_work/geomar/smomw452/GLORYS12/fieldsetC_")
else:
    variables = {"U": "vozocrtx", "V": "vomecrty", "W": "W", "S": "S", "T": "T"}
    fieldsetC = parcels.FieldSet.from_parcels(
        "/gxfs_work/geomar/smomw452/GLORYS12/fieldsetC_", extra_fields=variables
    )

# Prepare particle release
lon_release = lon  # longitude of release
lat_release = lat  # latitude of release
depth_release = depth  # depth of release, meters
time_release = release_times

pset = parcels.ParticleSet.from_list(
    fieldset=fieldsetC,
    pclass=custom_kernel.SampleParticle,
    lon=np.tile(lon_release, len(release_times)),
    lat=np.tile(lat_release, len(release_times)),
    depth=np.tile(depth_release, len(release_times)),
    time=np.repeat(release_times, len(lon_release)),
)

print(f"Number of particles: {len(pset)}")

outputfile = parcels.ParticleFile(
    "../data/test_trajectories.zarr",
    pset,
    timedelta(hours=12),
    chunks=(500 * 27 * 2 * 2, 365),
)  # timedelta was 6 before

# Defining kernel
adv = pset.Kernel(parcels.AdvectionRK4_3D)
age = pset.Kernel(custom_kernel.age)
sample = pset.Kernel(custom_kernel.sampling)
sample_UV = pset.Kernel(custom_kernel.velocity_sampling)
deleteparticle = pset.Kernel(custom_kernel.DeleteParticle_outside_domain_beached)

kernels = adv + sample + sample_UV + age + deleteparticle

# Run simulation for one time step
# to find particles that are on land.
# Initially all particle's temp is set to -100
# and in next timestep actual values are loaded
# where parcels on land will have temp == 0.
pset.execute(kernels, runtime=1)

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

# Get land_indices of future releases
# NOTE: this assumes future releases will be at the same locations

nb_releases = len(release_times)
all_land_ind = np.tile(land_indices, nb_releases) * np.repeat(
    np.arange(1, nb_releases + 1), len(land_indices)
)
# pset.remove_indices(all_land_ind)
# np.set_printoptions(threshold=np.inf)
print(all_land_ind)
for i in range(1, nb_releases):
    print(
        pset[land_indices[0]].lon,
        pset[land_indices[0]].lat,
        pset[land_indices[0]].depth,
    )
    print(
        pset[land_indices[0] + n_particles_per_release].lon,
        pset[land_indices[0] + n_particles_per_release].lat,
        pset[land_indices[0] + n_particles_per_release].depth,
    )
    land_indices += n_particles_per_release
    pset.remove_indices(land_indices)
    count = len(land_indices)
    print(
        f"{count} additional particles have been removed from the release at {release_times[i]}"
    )
    print(land_indices)


pset.execute(
    kernels,
    runtime=timedelta(days=365 * 27),
    dt=-timedelta(minutes=10),
    output_file=outputfile,
)
