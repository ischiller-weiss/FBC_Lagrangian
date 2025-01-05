# FBC Overflow pathways

## Running

To calculate the parcel trajectories, this project relies on [parcels](https://github.com/OceanParcels/Parcels). The experiment is defined in `experiment.py` with the kernels being defined in `kernel.py` and the `prcels.FieldSet` object is created in `create_fieldset.py`.

To reproduce the experiment the `run.sh` script can be run as follows:

```
cd <this/repository/src>
sbatch run.sh
```

The status of the run can be checked with

- `squeue -u $USER` to see the jobs-status
- `tail -f ../logs/parcels_<SLURM-JOBID>.out` to see the parcels progress

Cancel the job with `scancel <SLURM-JOBID>`, where `SLURM-JOBID` can be found with `squeue -u $USER`

## Model output
GLORYS12 Copernicus Reanalysis (1/12 degree) on the native C-grid.

## Experiment description

Release 10,000 particles every 5 days from 2020-05-01 to 2003-01-01 from the Faroe Bank Channel (S-section) ([Larsen et al., 2024](https://doi.org/10.1029/2024GL110097)) over 600 - 1100m depth range. Particles are backtracked in time for 27 years.
