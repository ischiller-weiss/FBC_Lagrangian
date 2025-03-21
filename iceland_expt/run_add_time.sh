#!/bin/bash
#SBATCH --job-name=run_add_time
#SBATCH --output=logs/add_time_%j.out
#SBATCH --error=logs/add_time_%j.out
#SBATCH --time=01-00:00:00
#SBATCH --mem=50G
#SBATCH --partition=data


source ~/.bashrc
conda activate ~/miniconda3/envs/py3_std_maps_2023-11-20
python add_time.py
