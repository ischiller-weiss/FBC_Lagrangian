#!/bin/bash
#SBATCH --job-name=parcels
#SBATCH --output=../logs/parcels_%j.out
#SBATCH --error=../logs/parcels_%j.out
#SBATCH --time=9-00:00:00
#SBATCH --ntasks=1
#SBATCH --mem=250G
#SBATCH --partition=base
#SBATCH --qos=long

source ~/.bashrc
conda activate ~/miniconda3/envs/py3_std_maps_2023-11-20
python experiment.py
