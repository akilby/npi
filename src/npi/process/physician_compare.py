import glob
import os

import pandas as pd

from ..constants import RAW_PC_DIR
from . import pc_col_dict, pc_col_dict_reverse, pc_colnames

# lf = ['Medical school name', 'Graduation year']


def expand_list_of_vars(lookfor):
    return ['NPI'] + [data_dict_reverse[x] for x in lookfor] + lookfor


def _proc_2016_07(filename, variable):
    df = pd.read_csv(filename, index_col=False, skiprows=[0], header=None)
    dropli = list((df.isnull().sum() == df.shape[0]).reset_index()[
        df.isnull().sum() == df.shape[0]]['index'].values)
    df = df.drop(columns=dropli)
    df.columns = pd.read_csv(filename, index_col=False, nrows=0).columns
    return df


def _proc_2014_12(filename, variable):
    df = pd.read_csv(filename,  engine="python", sep=',',
                     quotechar='"', error_bad_lines=False)
    return df


def _proc_2014_06(filename, variable):
    df = pd.read_csv(filename,  engine="python", sep=',',
                     quotechar='"', error_bad_lines=False,
                     header=None)
    df.columns = pc_colnames
    return df


def _proc_2014_03(filename, variable):
    df = pd.read_csv(filename,  engine="python", sep=',',
                     quotechar='"', error_bad_lines=False,
                     header=None)
    df.columns = pc_colnames
    return df


def _proc(filename, variable):
    df = pd.read_csv(
        filename,
        index_col=False,
        usecols=lambda x: str(x).strip() in expand_list_of_vars(variable))
    return df


def process_vars(lookfor, drop_duplicates=True, month_var=False):
    df_final = pd.DataFrame()
    for filename in glob.glob(os.path.join(RAW_PC_DIR, '*/*.csv')):
        try:
            if filename == os.path.join(RAW_PC_DIR,
                                        'Refresh_Data_Archive_07_2016/'
                                        'Refresh_Data_Archive_07_2016.csv'):
            elif filename == os.path.join(RAW_PC_DIR,
                                          'Refresh_Data_Archive_December_2014/'
                                          'National_Downloadable_File.csv'):
            elif (filename == os.path.join(RAW_PC_DIR,
                                           'Refresh_Data_Archive_June_2014/'
                                           'National_Downloadable_File.csv')
                  or filename == os.path.join(RAW_PC_DIR,
                                              'Refresh_Data_Archive_March_2014'
                                              '/National_Downloadable_File.csv'
                                              )):
                df = pd.read_csv(filename,  engine="python", sep=',',
                                 quotechar='"', error_bad_lines=False,
                                 header=None)
                df.columns = colnames
            else:
        except ValueError:
            print('FILENAME:', filename)
            raise Exception
        df.columns = [x.strip() for x in df.columns]
        df = df.rename(columns=data_dict)[['NPI'] + lookfor]
        df_final = df_final.append(df).drop_duplicates()
    return df_final
