import csv
import time
import os
import praw
import datetime
import argparse
import filecmp
import glob
import shutil
import collections
from praw.models import MoreComments
###################################################################################################################################


for year in {2007..2019}; do
  for month in `seq 1 12`; do
      wget https://data.nber.org/npi/byvar/$year/$month/ptaxcode$year$month.csv
    done
  done

for year in {2007..2019}; do
  for month in `seq 1 12`; do
      wget https://data.nber.org/npi/byvar/$year/$month/plocline1$year$month.csv
    done
  done

for year in {2007..2019}; do
  for month in `seq 1 12`; do
      wget https://data.nber.org/npi/byvar/$year/$month/plocline2$year$month.csv
    done
  done

for year in {2007..2019}; do
  for month in `seq 1 12`; do
      wget https://data.nber.org/npi/byvar/$year/$month/ploccityname$year$month.csv
    done
  done

for year in {2007..2019}; do
  for month in `seq 1 12`; do
      wget https://data.nber.org/npi/byvar/$year/$month/plocstatename$year$month.csv
    done
  done

for year in {2007..2019}; do
  for month in `seq 1 12`; do
      wget https://data.nber.org/npi/byvar/$year/$month/ploczip$year$month.csv
    done
  done


 