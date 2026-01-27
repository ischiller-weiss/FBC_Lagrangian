#!/gxfs_home/geomar/smomw452/miniconda3/envs/py3_std_maps_2023-11-20/bin/python
# SBATCH --job-name=climatology
# SBATCH --output=./logs/climatology_%j.out
# SBATCH --error=./logs/climatology_%j.out
# SBATCH --time=2:00:00
# SBATCH --ntasks=1
# SBATCH --mem=30G
# SBATCH --partition=data
"""
Extract and calculate daily climatology from GLORYS12 data.
Supports temperature (T) and salinity (S) variables.
Uses Dask for parallel processing.
Main logic extracted from daily_climatology.ipynb (cell 82+)
"""

import os
from glob import glob

import xarray as xr
from dask.distributed import Client, LocalCluster


def load_timeseries(inpath, variable):
    """
    Load and concatenate all data files for a given variable into a single time series.
    Uses dask for parallel loading.

    Parameters
    ----------
    inpath : str
        Path to directory containing variable subdirectories (e.g., T/, S/)
    variable : str
        Variable name ('T' for temperature or 'S' for salinity)

    Returns
    -------
    xr.Dataset
        Concatenated time series with dask arrays

    Raises
    ------
    FileNotFoundError
        If no files found for the specified variable
    """
    var_files = sorted(glob(f"{inpath}/{variable}/*.nc"))

    if not var_files:
        raise FileNotFoundError(f"No files found in {inpath}/{variable}/*.nc")

    print(f"Found {len(var_files)} {variable} files")
    print("Loading with dask...")

    # Use open_mfdataset with dask for parallel loading
    timeseries = xr.open_mfdataset(
        var_files,
        combine="by_coords",
        parallel=True,
        chunks={"time_counter": 1},  # ~1 month per chunk
        decode_coords=False,
    )

    # Clean attributes
    timeseries.attrs = {}
    for v in timeseries.variables:
        timeseries[v].attrs = {}

    return timeseries


def calculate_daily_climatology(timeseries, variable, output_path="./data"):
    """
    Calculate daily climatology from a time series.

    This is the main climatology calculation - groups data by day-of-year
    and computes the mean across all years. Uses dask for computation.

    Parameters
    ----------
    timeseries : xr.Dataset
        Input data with time_counter dimension
    variable : str
        Variable name ('T' for temperature or 'S' for salinity)
    output_path : str, optional
        Path to save the output zarr file (default: "./data")

    Returns
    -------
    tuple
        (climatology_per_doy, climatology_per_date)
        - climatology_per_doy: climatology indexed by day-of-year (1-365)
        - climatology_per_date: climatology expanded to match original time dimension
    """
    # Extract date information
    dates = timeseries.time_counter
    doy_matching_dates = dates.dt.dayofyear

    # Compute climatology by grouping on day-of-year
    print("Computing daily climatology (groupby + mean)...")
    climatology_per_doy = timeseries.groupby("time_counter.dayofyear").mean(
        "time_counter"
    )

    # Expand climatology back to match the original time dimension
    print("Expanding climatology to full time dimension...")
    climatology_per_doy_unrolled = climatology_per_doy.reindex(
        dayofyear=doy_matching_dates.values
    )
    climatology_per_date = climatology_per_doy_unrolled.rename(
        {"dayofyear": "time_counter"}
    )
    climatology_per_date["time_counter"] = dates.values

    # Set time origin metadata
    climatology_per_date["time_counter"].attrs["time_origin"] = "1950-JAN-01 00:00:00"

    # Save the compact 365-day climatology to zarr
    os.makedirs(output_path, exist_ok=True)
    output_file = f"{output_path}/{variable}_daily_climatology_365days_land_mask.zarr"
    print(f"Saving {variable} climatology to {output_file}...")
    climatology_per_doy.to_zarr(output_file)
    print(f"✓ Saved {variable} climatology")

    return climatology_per_doy, climatology_per_date


def main(n_workers=None, threads_per_worker=1, memory_limit="20GB"):
    """
    Main execution function with dask cluster support.

    Parameters
    ----------
    n_workers : int, optional
        Number of dask workers (default: number of CPU cores)
    threads_per_worker : int, optional
        Number of threads per worker (default: 1, use processes instead)
    memory_limit : str, optional
        Memory limit per worker (default: '4GB')
    """
    # Configuration
    inpath = "/gxfs_work/geomar/smomw452/GLORYS12/Data/"
    output_path = "./data/climatology/"

    # Initialize dask LocalCluster
    print("Initializing Dask LocalCluster...")
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=memory_limit,
        processes=True,
    )
    client = Client(cluster)

    print(f"Dask Dashboard: {client.dashboard_link}")
    print(f"Cluster info:\n{cluster}")

    try:
        results = {}

        # Process both temperature and salinity
        for variable in ["T", "S"]:
            print(f"\n{'='*60}")
            print(f"Processing {variable}")
            print("=" * 60)

            # Load time series
            print(f"Loading {variable} time series...")
            timeseries = load_timeseries(inpath, variable)
            print(f"{variable} time series shape: {timeseries.dims}")
            print(timeseries)

            # Calculate daily climatology
            print(f"\nCalculating daily climatology for {variable}...")
            clim_doy, clim_date = calculate_daily_climatology(
                timeseries, variable=variable, output_path=output_path
            )

            results[variable] = {
                "climatology_per_doy": clim_doy,
                "climatology_per_date": clim_date,
            }

            print(f"\nDaily climatology for {variable} (365 days):")
            print(clim_doy)

        print(f"\n{'='*60}")
        print("✓ All climatologies computed successfully!")
        print("=" * 60)

        return results

    finally:
        # Clean up dask cluster
        print("\nClosing Dask cluster...")
        client.close()
        cluster.close()


if __name__ == "__main__":
    # Optional: customize cluster settings
    main(n_workers=16, threads_per_worker=1, memory_limit="20GB")
