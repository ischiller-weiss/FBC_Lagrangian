#!/bin/bash
#SBATCH --job-name=run_save_positions
#SBATCH --output=logs/run_positions_%j.out
#SBATCH --error=logs/run_positions_%j.out
#SBATCH --time=01-00:00:00
#SBATCH --mem=50G
#SBATCH --partition=data


source ~/.bashrc
conda activate ~/miniconda3/envs/py3_std_maps_2023-11-20
python save_positions_iceland.py
