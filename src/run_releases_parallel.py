#!/usr/bin/env bash
# SBATCH --job-name=parallel_parcels
# SBATCH --ntasks=1273
# SBATCH --cpus-per-task=1
# SBATCH --mem-per-cpu=128G
# SBATCH --time=30:00:00
# SBATCH --partition=base
"""
Run a parcels simulation for each release time in parallel
"""

import subprocess

import pandas as pd
import tqdm as tqdm

release_times = pd.date_range(start="1993-01-01", end="1995-01-01", freq="5D", unit="s")

for release_time in tqdm.tqdm(release_times):
    subprocess.run(
        [
            "srun",
            " --ntasks=1",
            "--exclusive",
            "python",
            "src/experiment.py",
            "--release_start",
            release_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "--release_end",
            release_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "--frequency",
            "5D",
        ]
    )
