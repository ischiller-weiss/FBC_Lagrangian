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
python experiment.py --release_start 1993-01-01T00:00:00 --release_end 2000-12-31T00:00:00 --seeding 2Duniform --frequency 2MS --output_dir ../data/greenlandsea_forward_release/ --lon_bds -10, 4 --lat_bds 77, 73 --start_depth 0 --end_depth 1000
