#!/bin/bash
#SBATCH --job-name=rechunk_zarr
#SBATCH --output=/gxfs_work/geomar/smomw452/GLORYS12/revisions/logs_rev/rechunk_%j.out
#SBATCH --error=/gxfs_work/geomar/smomw452/GLORYS12/revisions/logs_rev/rechunk_%j.err
#SBATCH --time=01-20:00:00
#SBATCH --mem=100G
#SBATCH --partition=data

source ~/.bashrc
conda activate ~/miniconda3/envs/py3_std_maps_2023-11-20
python rechunk_stage_2_prep.py
