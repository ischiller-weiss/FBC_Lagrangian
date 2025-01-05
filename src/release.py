import kernel as kfuncs
import numpy as np
from parcels import ParticleSet, AdvectionRK4_3D, ParticleFile
from datetime import timedelta

n_particles_per_release = 10_000
lon_bds = (-6.5, -2.5)
lat_bds = (61.3, 60.3)

lon=np.random.uniform(*lon_bds, size=(n_particles_per_release, ))
lat=np.random.uniform(*lat_bds, size=(n_particles_per_release, ))
depth = np.random.uniform(650, 1100, size=(n_particles_per_release, ))

release_times = np.datetime64('1993-01-01') + np.arange(28 * 73) * np.timedelta64(5 * 24 * 3600, 's')
release_times = release_times[1:2004]

lon_release = lon # longitude of release
lat_release = lat # latitude of release
depth_release = depth  # depth of release, meters
time_release = release_times


pset = ParticleSet.from_list(fieldset=fieldsetC,
                                pclass=kfuncs.SampleParticle,
                                lon=np.tile(lon_release, len(release_times)),
                                lat=np.tile(lat_release, len(release_times)),
                                depth=np.tile(depth_release, len(release_times)),
                                time=np.repeat(release_times, len(lon_release)))

# Defining kernel
adv = pset.Kernel(AdvectionRK4_3D)
age = pset.Kernel(kfuncs.age)
sample = pset.Kernel(kfuncs.sampling)
sample_UV = pset.Kernel(kfuncs.velocity_sampling)
deleteparticle = pset.Kernel(kfuncs.DeleteParticle_outside_domain_beached)

kernels = adv+sample+sample_UV+age+deleteparticle

outputfile = ParticleFile('V_sec_backwards_release_5_days_backward.zarr',pset,timedelta(hours=24), chunks=(len(pset),2_000)) # timedelta was 6 before 

pset.execute(kernels, runtime=timedelta(days=365*27), dt=-timedelta(minutes=10), output_file=outputfile)