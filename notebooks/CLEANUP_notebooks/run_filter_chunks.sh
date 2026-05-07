#!/bin/bash
#SBATCH --job-name=filter_particles
#SBATCH --output=/gxfs_work/geomar/smomw452/GLORYS12/revisions/logs_rev/filter_%j.out
#SBATCH --error=/gxfs_work/geomar/smomw452/GLORYS12/revisions/logs_rev/filter_%j.err
#SBATCH --partition=base
#SBATCH --time=12:00:00
#SBATCH --mem=128GB
#SBATCH --cpus-per-task=4

echo "Job started: $(date)"
echo "Running on node: $(hostname)"

source ~/miniconda3/etc/profile.d/conda.sh
conda activate py3_std_maps_2023-11-20

python filter_and_save_particles_chunks.py

echo "Job finished: $(date)"
