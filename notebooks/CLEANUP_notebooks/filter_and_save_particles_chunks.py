#!/usr/bin/env python3
"""
Filter particles by initial density and save to zarr in chunks.
Run as: sbatch run_filter.sh

Each chunk is fully computed into memory before saving — avoiding
worker-to-worker communication issues during the zarr write.

If the job times out, resubmit — already completed chunks are skipped
automatically and the job picks up where it left off.

Set TEST_MODE = True to run on a small subset first and verify output.
Set TEST_MODE = False for the full run.
"""

import functools
import os
import warnings

import dask
import dask.distributed
import dask_jobqueue
import numpy as np
import seawater as gsw
import xarray as xr
import zarr

warnings.filterwarnings("ignore")

# Force all print statements to flush immediately (needed for sbatch logging)
print = functools.partial(print, flush=True)

print("Script started")

# ─── Test mode ───────────────────────────────────────────────────────────────
TEST_MODE = False  # set to False for full run
TEST_N = 100_000  # 100k since first 1k has no overflow particles
CHUNK_SIZE = 25_000  # 25k trajectories per chunk (~14.7 GB each in memory)

# ─── Dask cluster ────────────────────────────────────────────────────────────

print("Setting up Dask cluster...")
cluster = dask_jobqueue.SLURMCluster(
    cores=4,
    memory="24GB",
    processes=1,
    queue="base",
    walltime="12:00:00",
    interface="ib0",
    local_directory="$TMPDIR",
    log_directory="/gxfs_work/geomar/smomw452/GLORYS12/revisions/logs_rev/",
)

client = dask.distributed.Client(cluster)
cluster.scale(jobs=12)

print("Waiting for workers...")
try:
    client.wait_for_workers(10, timeout=300)
except Exception as e:
    print(f"Warning: {e} — continuing with available workers")
print(client)

# ─── Functions ───────────────────────────────────────────────────────────────


def filter_beached_particles(ds, dim=None):
    """Remove particles that reach zero salinity (beached).
    Uses isel instead of where to cleanly drop trajectories with no NaNs left behind.
    """
    particles_reach_zero_salinity = (ds.salt == 0).any(dim)
    particles_not_beached = ~particles_reach_zero_salinity
    ds_filtered = ds.isel(trajectory=particles_not_beached.compute())
    ds_filtered["trajectory"] = np.arange(ds_filtered.trajectory.size)
    return ds_filtered


def compute_density(ds):
    """Compute potential density (sigma0) and add to dataset."""
    ds_density = (
        xr.apply_ufunc(
            gsw.eos80.pden,
            ds.salt,
            ds.temp,
            0,
            dask="parallelized",
            output_dtypes=[np.float64],
        )
        - 1000
    )
    return ds.assign(sigma0=ds_density)


def filter_particles_by_initial_density(ds, density_threshold):
    """Keep only particles with sigma0 >= density_threshold at obs=0.
    Uses isel to cleanly drop trajectories with no NaNs left behind.
    """
    initial_density = ds.sigma0.isel(obs=0, drop=True)
    particles_above_threshold = initial_density >= density_threshold
    ds_filtered = ds.isel(trajectory=particles_above_threshold.compute())
    ds_filtered["trajectory"] = np.arange(ds_filtered.trajectory.size)
    return ds_filtered


def chunk_is_complete(chunk_path):
    """Check if a chunk zarr exists and has a valid .zmetadata file —
    indicating it was fully written and not just partially created."""
    return os.path.exists(chunk_path) and os.path.exists(
        os.path.join(chunk_path, ".zmetadata")
    )


# ─── Paths ───────────────────────────────────────────────────────────────────

input_path = (
    "/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/data/"
    "level2_uniform_release_clim_land_nans/"
    "uniform_parcels_releases_seed-2345_2000-2020.zarr"
)

output_path = (
    "/gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/data/"
    "level2_uniform_release_clim_land_nans/"
    "density_filtered_uniform_parcels_release_2000-2020.zarr"
)

# ─── Main ────────────────────────────────────────────────────────────────────

print("Loading dataset...")
ds_orig = xr.open_dataset(input_path, engine="zarr", chunks="auto")

if TEST_MODE:
    print(f"TEST MODE: subsetting to first {TEST_N:,} trajectories")
    ds_orig = ds_orig.isel(trajectory=slice(0, TEST_N))
    output_path = output_path.replace(".zarr", "_test.zarr")
    print(f"TEST MODE: output path → {output_path}")
else:
    print("FULL RUN: using all trajectories")

print(f"Input trajectories: {ds_orig.trajectory.size:,}")

print("Filtering beached particles...")
ds_filtered = filter_beached_particles(ds_orig, "obs")
print(f"After beaching filter: {ds_filtered.trajectory.size:,} trajectories")
print(
    f"NaNs at obs=0 after beaching: {ds_filtered.lat.isel(obs=0).isnull().sum().compute().values:,}"
)

print("Computing density...")
ds_density = compute_density(ds_filtered)

print("Filtering by initial density (sigma0 >= 28.0 at obs=0)...")
ds_density_filtered = filter_particles_by_initial_density(
    ds_density, density_threshold=28.0
)
print(f"After density filter: {ds_density_filtered.trajectory.size:,} trajectories")
print(
    f"NaNs at obs=0 after density: {ds_density_filtered.lat.isel(obs=0).isnull().sum().compute().values:,}"
)

# Add start_time as a plain coordinate
print("Adding start_time coordinate...")
p = ds_density_filtered
p["start_time"] = p.isel(obs=0).time
p = p.set_coords("start_time")

# Set up compressor
compressor = zarr.Blosc(cname="zstd", clevel=3, shuffle=2)
encoding = {var: {"compressor": compressor} for var in p.data_vars}
print(f"Encoding vars: {list(encoding.keys())}")

# ─── Save in chunks ──────────────────────────────────────────────────────────
# Each chunk is fully computed into memory before saving.
# Already completed chunks (with valid .zmetadata) are skipped automatically
# so the job can be resubmitted if it times out.

n_trajectories = p.trajectory.size
n_chunks = int(np.ceil(n_trajectories / CHUNK_SIZE))
print(
    f"\nSaving {n_trajectories:,} trajectories in {n_chunks} chunks of {CHUNK_SIZE:,}..."
)

# Report how many chunks are already done
chunk_paths = []
n_existing = 0
for i in range(n_chunks):
    cp = output_path.replace(".zarr", f"_chunk_{i:04d}.zarr")
    chunk_paths.append(cp)
    if chunk_is_complete(cp):
        n_existing += 1

print(f"{n_existing}/{n_chunks} chunks already completed — skipping those")
print(f"{n_chunks - n_existing}/{n_chunks} chunks remaining to save\n")

for i in range(n_chunks):
    start = i * CHUNK_SIZE
    end = min((i + 1) * CHUNK_SIZE, n_trajectories)
    chunk_path = chunk_paths[i]

    # Skip if chunk already fully written
    if chunk_is_complete(chunk_path):
        print(f"Chunk {i+1}/{n_chunks} already exists — skipping ({start:,}–{end:,})")
        continue

    print(
        f"Saving chunk {i+1}/{n_chunks}: trajectories {start:,}–{end:,} → {chunk_path}"
    )

    p_chunk = p.isel(trajectory=slice(start, end))

    # Force compute into memory before saving
    print(f"  Computing chunk {i+1}/{n_chunks} into memory...")
    p_chunk = p_chunk.compute()

    print(f"  Writing chunk {i+1}/{n_chunks} to zarr...")
    p_chunk.to_zarr(chunk_path, mode="w", encoding=encoding)

    print(f"Chunk {i+1}/{n_chunks} done! ({end - start:,} trajectories)")

print(f"\nAll {n_chunks} chunks saved!")

# ─── Spot-check a few chunks ─────────────────────────────────────────────────

print("\nSpot-checking chunks...")
for i in [0, n_chunks // 2, n_chunks - 1]:
    cp = chunk_paths[i]
    c = xr.open_zarr(cp)
    nans = c.lat.isel(obs=0).isnull().sum().values
    sigma_min = float(c.sigma0.isel(obs=0).min().values)
    print(
        f"  Chunk {i:04d}: {c.trajectory.size:,} trajectories, "
        f"NaNs at obs=0: {nans:,}, min sigma0: {sigma_min:.3f}"
    )

if TEST_MODE:
    print(
        "\nTEST MODE complete — if output looks correct set TEST_MODE = False and resubmit"
    )
else:
    print("\nFULL RUN complete!")
    print("Next step: run combine_chunks.py to merge all chunks into a single zarr")

# ─── Note for reloading ──────────────────────────────────────────────────────
# To reload all chunks as a single dataset:
#
# import glob, xarray as xr, numpy as np
# chunk_paths = sorted(glob.glob(output_path.replace(".zarr", "_chunk_*.zarr")))
# chunks = [xr.open_zarr(cp).reset_index("trajectory")
#                            .reset_coords("trajectory", drop=True)
#           for cp in chunk_paths]
# ds = xr.concat(chunks, dim="trajectory")
# ds["trajectory"] = np.arange(ds.trajectory.size)
# ds = ds.set_xindex("start_time")  # optional
