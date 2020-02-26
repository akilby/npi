import os

import pandas as pd

from .constants import NBER_PATH
from .utils import urlread


def get_file_list(year, month):
    df = pd.read_csv(os.path.join(NBER_PATH, str(year), str(month)))
    df.columns = ['nppes_var']
    return (pd.DataFrame(
        df.nppes_var
          .str.split('>').str[2]
          .str.split('<').str[0]
          .str.split('.').str[0]
          .drop_duplicates()
          .str.split('%s%s' % (year, month)).str[0]
          .drop_duplicates()
          .str.split('desc_').str[0]
          .dropna()).query('nppes_var!="" & nppes_var!=" "')
                    .reset_index(drop=True)
                    .sort_values('nppes_var'))['nppes_var']


def get_file_info(year, month, varname):
    url = os.path.join(NBER_PATH,
                       str(year),
                       str(month),
                       'desc_%s%s%s.txt' % (varname, year, month))
    urlread(url)
