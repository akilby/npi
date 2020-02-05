#!/bin/bash

#SBATCH --mem=0
#SBATCH --exclusive
#SBATCH --time=1-00:00:00
#SBATCH --partition=reservation
#SBATCH --reservation=kilby
#SBATCH --output=pyscript.out
#SBATCH --error=pyscript.out
module add python/3.7.3-base
source  ~/venvs/general2/bin/activate
python -u /home/amin.p/code/npi/MergeFiles.py
deactivate