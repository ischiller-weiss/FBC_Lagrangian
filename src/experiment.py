#!/gxfs_home/geomar/smomw452//miniconda3/envs/py3_std_maps_2023-11-20/bin/python
import dask, dask.distributed
import dask_jobqueue
import parcels
import numpy as np
import os
from pathlib import Path
from datetime import timedelta
import xarray as xr
import calendar
import warnings
warnings.filterwarnings("ignore")
import numpy as np
from glob import glob
import tqdm as tqdm
import cf
import logging
import pandas as pd

import create_fieldset as custom_fieldset
import kernel import custom_kernel


# Set up dask cluster
cluster = dask_jobqueue.SLURMCluster(

    # Dask worker size
    cores=4, memory='16GB',
    processes=1, # Dask workers per job
    
    # SLURM job script things
    queue='base', walltime='04:00:00',
    
    # Dask worker network and temporary storage
    interface='ib0', local_directory='$TMPDIR',  # for spilling tmp data to disk
    log_directory='slurm/'
)

client = dask.distributed.Client(cluster)
cluster.adapt(minimum=5, maximum=5)

print(client)
print("To connect to the dask dashboard you might need to do port forwarding to the exact same node this script is running on, e.g. `ssh -L 8787:localhost:8787 this.node.com`")

# Set random seed
np.random.seed(2345)

# Settings

n_particles_per_release = 10_000

lon_bds = (-6.5, -2.5)
lat_bds = (61.3, 60.3)

lon=np.random.uniform(*lon_bds, size=(n_particles_per_release, ))
lat=np.random.uniform(*lat_bds, size=(n_particles_per_release, ))
depth = np.random.uniform(650, 1100, size=(n_particles_per_release, ))

release_times = pd.date_range(start='2003-01-01', end='2020-06-03', freq='5D', unit='s')

logging.info(f"Release times: {release_times}")

# Model filenames
inpath='/gxfs_work/geomar/smomw452/GLORYS12/'

max_ind = None
min_ind = 2 # start from 1993!

ufiles, vfiles, wfiles, sfiles, tfiles = custom_fieldset.get_files(inpath, min_ind=min_ind, max_ind=max_ind)
logging.info(f"Number of files: {len(ufiles)}")

coords, variables, filenames, dimensions = custom_fieldset.create_mapping()

if not os.path.exists('../fieldsetC_U.nc'):
    ds = custom_fieldset.create_dataset(ufiles, vfiles, wfiles, sfiles, tfiles)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", parcels.FileWarning)
        fieldsetC = parcels.FieldSet.from_nemo(ds, variables, dimensions,
            allow_time_extrapolation=False)
    
    fieldsetC.write('../fieldsetC_')
else:
    variables = {'U': 'vozocrtx', 'V': 'vomecrty', 'W': 'W', 'S': 'S', 'T': 'T'}
    fieldsetC = parcels.FieldSet.from_parcels('../fieldsetC_', extra_fields=variables)
    
# Prepare particle release
lon_release = lon # longitude of release
lat_release = lat # latitude of release
depth_release = depth  # depth of release, meters
time_release = release_times

pset = parcels.ParticleSet.from_list(fieldset=fieldsetC,
                                pclass=custom_kernel.SampleParticle,
                                lon=np.tile(lon_release, len(release_times)),
                                lat=np.tile(lat_release, len(release_times)),
                                depth=np.tile(depth_release, len(release_times)),
                                time=np.repeat(release_times, len(lon_release)))

outputfile = parcels.ParticleFile('../data/parcels_trajectories.zarr', pset, timedelta(hours=12), chunks=(500*27*2*2,365)) # timedelta was 6 before

# Defining kernel
adv = pset.Kernel(parcels.AdvectionRK4_3D)
age = pset.Kernel(custom_kernel.age)
sample = pset.Kernel(custom_kernel.sampling)
sample_UV = pset.Kernel(custom_kernel.velocity_sampling)
deleteparticle = pset.Kernel(custom_kernel.DeleteParticle_outside_domain_beached)

kernels = adv+sample+sample_UV+age+deleteparticle

pset.execute(kernels, runtime=timedelta(days=365*27), dt=-timedelta(minutes=10), output_file=outputfile)

