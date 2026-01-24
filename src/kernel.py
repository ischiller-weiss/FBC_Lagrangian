"""
Parcel kernel functions
"""

import math
from operator import attrgetter

import numpy as np
from parcels import JITParticle, Variable


class SampleParticle(JITParticle):
    temp = Variable("temp", dtype=np.float32, initial=-100)  # fieldsetC.T
    salt = Variable("salt", dtype=np.float32, initial=-100)
    age = Variable("age", dtype=np.float32, initial=0)
    uvel = Variable("uvel", dtype=np.float32, initial=0)
    vvel = Variable("vvel", dtype=np.float32, initial=0)
    distance = Variable("distance", dtype=np.float32, initial=0.0)
    prev_lon = Variable(
        "prev_lon", dtype=np.float32, to_write=False, initial=attrgetter("lon")
    )
    prev_lat = Variable(
        "prev_lat", dtype=np.float32, to_write=False, initial=attrgetter("lat")
    )


def DeleteErrorParticle(particle, fieldset, time):
    if particle.state >= 40:  # deletes every particle that throws an error
        particle.delete()


def age(particle, fieldset, time):
    """Calculate age of particles as time after release in hours"""
    particle.age += particle.dt / 3600


def velocity_sampling(particle, fieldset, time):
    """Sample velocity."""
    (particle.uvel, particle.vvel) = fieldset.UV[
        time, particle.depth, particle.lat, particle.lon
    ]


def sampling(particle, fieldset, time):
    """Sample temperature & salinity."""
    particle.temp = fieldset.T[time, particle.depth, particle.lat, particle.lon]
    particle.salt = fieldset.S[time, particle.depth, particle.lat, particle.lon]


def TotalDistance(particle, fieldset, time):
    lat_dist = (particle.lat - particle.prev_lat) * 1.11e2
    lon_dist = (
        (particle.lon - particle.prev_lon)
        * 1.11e2
        * math.cos(particle.lat * math.pi / 180)
    )

    particle.distance += math.sqrt(lon_dist**2 + lat_dist**2)

    particle.prev_lon = particle.lon
    particle.prev_lat = particle.lat


def CheckError(particle, fieldset, time):
    if particle.state >= 50:
        particle.delete()


# combining delete error and beached particles
def DeleteParticle_outside_domain_beached(particle, fieldset, time):
    if particle.state >= 40:  # Check for error state
        print(
            "Particle [%d] lost due to %d error !! (%g %g %g %g)"
            % (
                particle.id,
                particle.state,
                particle.lon,
                particle.lat,
                particle.depth,
                particle.time,
            )
        )
        particle.delete()
    elif particle.depth < 0:  # Check for beached state
        print(
            "Particle [%d] lost due to beaching !! (%g %g %g %g)"
            % (particle.id, particle.lon, particle.lat, particle.depth, particle.time)
        )
        particle.delete()
