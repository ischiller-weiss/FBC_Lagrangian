#!/bin/bash
#SBATCH --job-name=parcels
#SBATCH --output=../logs/parcels.out
#SBATCH --error=../logs/parcels.err
#SBATCH --time=10-00:00:00
#SBATCH --ntasks=1
#SBATCH --mem=200G

source activate ~/miniconda3/envs/py3_std_maps_2023-11-20
python experiment.py
