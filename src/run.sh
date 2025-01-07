#!/bin/bash
#SBATCH --job-name=parc_dask
#SBATCH --output=../logs/parcels_%j.out
#SBATCH --error=../logs/parcels_%j.out
#SBATCH --time=20-00:00:00
#SBATCH --ntasks=1
#SBATCH --mem=30G
#SBATCH --partition=data

source ~/.bashrc
conda activate ~/miniconda3/envs/py3_std_maps_2023-11-20
echo "Running parcels"
python experiment.py
