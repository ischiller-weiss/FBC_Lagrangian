#!/bin/bash
#SBATCH --job-name=parc_dask
#SBATCH --output=../logs/parcels_%j.out
#SBATCH --error=../logs/parcels_%j.out
#SBATCH --time=20-00:00:00
#SBATCH --ntasks=1
#SBATCH --mem=30G
#SBATCH --partition=data
#SBATCH --exclude=nesh-dm01

source ~/.bashrc
source /gxfs_work/geomar/smomw452/GLORYS12/schillerweiss_2025/.venv/bin/activate
echo "Running parcels"
python experiment.py --release_start 2011-01-01T00:00:00 --release_end 2015-12-31T00:00:00 --seeding uniform --frequency 5D --output_dir ../data/uniform_release_clim_land_nans/
