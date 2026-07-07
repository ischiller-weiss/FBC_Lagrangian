import math

import numpy as np


def haversine(coord1, coord2):
    """Calculate the Haversine distance between two coordinates in decimal degrees."""
    # Convert decimal degrees to radians
    lat1, lon1 = np.radians(coord1)
    lat2, lon2 = np.radians(coord2)

    # Haversine formula
    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))

    # Radius of Earth in kilometers (mean radius)
    r = 6371.0
    return r * c


def interpolate_coordinates(coord1, coord2, n):
    """Interpolate n evenly spaced coordinates between two geographical points."""
    # Calculate total Haversine distance
    total_distance = haversine(coord1, coord2)

    # Calculate spacing
    spacing_km = total_distance / (n - 1)

    # Calculate lat/lon differences
    lat1, lon1 = np.radians(coord1)
    lat2, lon2 = np.radians(coord2)

    # Interpolated coordinates
    interpolated_coords = []
    for i in range(n):
        f = i / (n - 1)  # Fraction along the line
        a = math.sin((1 - f) * total_distance / 6371.0) / math.sin(
            total_distance / 6371.0
        )
        b = math.sin(f * total_distance / 6371.0) / math.sin(total_distance / 6371.0)

        x = a * math.cos(lat1) * math.cos(lon1) + b * math.cos(lat2) * math.cos(lon2)
        y = a * math.cos(lat1) * math.sin(lon1) + b * math.cos(lat2) * math.sin(lon2)
        z = a * math.sin(lat1) + b * math.sin(lat2)

        lat = math.atan2(z, math.sqrt(x**2 + y**2))
        lon = math.atan2(y, x)

        # Convert back to degrees
        interpolated_coords.append((np.degrees(lat), np.degrees(lon)))

    return spacing_km, interpolated_coords
