import calendar
import glob
import os
import re

import pandas as pd

from ..constants import RAW_PC_DIR
from ..utils.utils import convert_dtypes, force_integer, singleton
from . import DTYPES, PC_COL_DICT, PC_COL_DICT_REVERSE, PC_COLNAMES

# lf = ['Medical school name', 'Graduation year']


def expand_list_of_vars(variables):
    return ['NPI'] + [PC_COL_DICT_REVERSE[x] for x in variables] + variables


def detect_date(string):
    """
    Detects dates from filenames of the physician compare data
    """
    try:
        date = singleton(
            set(re.findall(r'20[0-9]{2}-[0-9]{2}-[0-9]{2}', string)))
        year, month = None, None
        return pd.to_datetime(date)
    except AssertionError:
        date = None
    except ValueError:
        date = None
    if not date:
        monthnames = ([calendar.month_abbr[x] for x in range(1, 13)]
                      + [calendar.month_name[x] for x in range(1, 13)])
        dictnames = {**dict((v, k) for k, v in enumerate(calendar.month_name)),
                     **dict((v, k) for k, v in enumerate(calendar.month_abbr))}
        year = singleton(set(re.findall(r'20[0-9]?[0-9]?', string)))
        try:
            month = singleton(
                set([dictnames[x] for x in monthnames if x in string]))
        except AssertionError:
            month = None
        if not month:
            try:
                month_cand = int(singleton(set(
                    re.findall(r'_[0-9]?[0-9]?_', string))).replace('_', ''))
                if month_cand in range(1, 13):
                    month = month_cand
            except AssertionError:
                month = None
        return pd.to_datetime(f'{year}-{month}-01')


class ReadPhysicianCompare(object):

    @staticmethod
    def _proc_2014_03_01(filename, variables):
        df = pd.read_csv(filename,  engine="python", sep=',',
                         quotechar='"', error_bad_lines=False,
                         header=None)
        df.columns = PC_COLNAMES
        df['Graduation year'] = df['Graduation year'].apply(
            lambda x: force_integer(x)).astype('Int64')
        return df

    @staticmethod
    def _proc_2014_06_01(filename, variables):
        df = pd.read_csv(filename,  engine="python", sep=',',
                         quotechar='"', error_bad_lines=False,
                         header=None)
        df.columns = PC_COLNAMES
        df = df.drop(0).reset_index(drop=True)
        df['Graduation year'] = df['Graduation year'].apply(
            lambda x: force_integer(x)).astype('Int64')
        return df

    @staticmethod
    def _proc_2014_12_01(filename, variables):
        df = pd.read_csv(filename,  engine="python", sep=',',
                         quotechar='"', error_bad_lines=False)
        df['Graduation year'] = df['Graduation year'].apply(
            lambda x: force_integer(x)).astype('Int64')
        return df

    @staticmethod
    def _proc_2016_07_01(filename, variables):
        df = pd.read_csv(filename, index_col=False, skiprows=[0], header=None)
        dropli = list((df.isnull().sum() == df.shape[0]).reset_index()[
            df.isnull().sum() == df.shape[0]]['index'].values)
        df = df.drop(columns=dropli)
        df.columns = pd.read_csv(filename, index_col=False, nrows=0).columns
        return df

    @staticmethod
    def _proc(filename, variables):
        df = pd.read_csv(
            filename,
            index_col=False,
            usecols=lambda x: str(x).strip() in expand_list_of_vars(variables))
        return df


def process_vars(variables, drop_duplicates=True, date_var=False):
    """
    Pulls in physician compare data by variables, appends
    """
    df_final = pd.DataFrame()
    for filename in glob.glob(os.path.join(RAW_PC_DIR, '*/*.csv')):
        date = detect_date(filename)
        name = str(date).split(' ')[0].replace('-', '_')
        try:
            # Gets special proc function if one exists,
            # otherwise uses defalt of _proc()
            default = getattr(ReadPhysicianCompare, '_proc')
            func = getattr(ReadPhysicianCompare, f'_proc_{name}', default)
            df = func(filename, variables)
        except ValueError:
            print('FILENAME:', filename)
            raise ValueError
        df.columns = [x.strip() for x in df.columns]
        if date_var:
            df['date'] = date
        df = df.rename(columns=PC_COL_DICT)[['NPI'] + variables]
        try:
            df = convert_dtypes(df, DTYPES)
        except ValueError:
            print(filename)
            raise ValueError
        except TypeError:
            print(filename)
            raise TypeError
        df_final = df_final.append(df)
        if drop_duplicates:
            df_final = df_final.drop_duplicates()
    return df_final.reset_index(drop=True)
