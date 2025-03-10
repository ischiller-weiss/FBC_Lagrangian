#!/bin/bash
#SBATCH --job-name=rechunk_zarr
#SBATCH --output=logs/rechunk_%j.out  # Specify output file
#SBATCH --error=logs/rechunk_%j.err   # Specify error file
#SBATCH --time=01-20:00:00
#SBATCH --mem=100G
#SBATCH --partition=data

source ~/.bashrc
conda activate ~/miniconda3/envs/py3_std_maps_2023-11-20
echo "Rechunking now..."
python rechunk_iceland.py
