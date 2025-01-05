#!/bin/bash
#SBATCH --job-name=parcels
#SBATCH --output=../logs/parcels_%j.out
#SBATCH --error=../logs/parcels_%j.out
#SBATCH --time=0-01:00:00
#SBATCH --ntasks=1
#SBATCH --mem=250G
#SBATCH --partition=interactive


source ~/.bashrc
conda activate ~/miniconda3/envs/py3_std_maps_2023-11-20
python experiment.py
