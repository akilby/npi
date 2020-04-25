"""
No. Not every individual or group practice in PECOS is included on Physician
Compare. In order to be included on the site, individual health care
professionals must also:

- Have at least one practice location address in PECOS
- Have at least one specialty in PECOS
- Have submitted a Medicare claim within the last 12 months or be newly
enrolled in PECOS within the last 6 months

For group practices to appear on Physician Compare, at least two active
Medicare eligible professionals (EPs) must reassign their benefits to the
group’s TIN.
"""

import calendar
import glob
import os
import re

import pandas as pd

from ..constants import RAW_PC_DIR
from ..utils.utils import (convert_dtypes, force_integer, force_integer_blanks,
                           singleton)
from . import DTYPES, PC_COL_DICT, PC_COL_DICT_REVERSE, PC_COLNAMES

# lf = ['Medical school name', 'Graduation year']


def expand_list_of_vars(variables):
    mapping = pd.DataFrame.from_dict(PC_COL_DICT, orient='index')
    othlist = (mapping.reset_index()
                      .merge(pd.DataFrame({0: variables}))['index']
                      .values.tolist())
    # othlist = [PC_COL_DICT_REVERSE[x] for x in variables]
    return ['NPI'] + othlist + variables


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
            usecols=(lambda x: str(x).strip().lower()
                     in [x.lower() for x in expand_list_of_vars(variables)]))
        return df


def physician_compare_select_vars(variables,
                                  drop_duplicates=True,
                                  date_var=False):
    """
    Pulls in physician compare data by variables, appends
    if a variable hasn't been added to DTYPES, then it will not run
    and adding it may cause problems as that var hasn't been debugged
    """
    # NPI will be auto-added to the variable list later
    variables = [x for x in variables if x != 'NPI']
    df_final = pd.DataFrame()
    for filename in glob.glob(os.path.join(RAW_PC_DIR, '*/*.csv')):
        print('RUNNING:', filename)
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
        full_col_ren = {**PC_COL_DICT, **{key.lower(): val
                                          for key, val in PC_COL_DICT.items()}}
        df = df.rename(columns=full_col_ren)
        if 'Phone Number' not in variables:
            # for some reason phone number is missing in some datasets
            variables_use = [x for x in variables if x != 'Phone Number']
        df = df[['NPI'] + variables_use]
        try:
            df = convert_dtypes(df, DTYPES)
        except ValueError:
            print(filename)
            raise ValueError
        except TypeError:
            print(filename)
            try:
                var = 'Group Practice PAC ID'
                if var in variables_use:
                    df[var] = df[var].apply(
                        lambda x: force_integer_blanks(x, ' '))
                    df = convert_dtypes(df, DTYPES)
            except TypeError:
                try:
                    var = 'Number of Group Practice members'
                    if var in variables_use:
                        df[var] = df[var].apply(
                            lambda x: force_integer_blanks(x, '.'))
                        df = convert_dtypes(df, DTYPES)
                except TypeError:
                    raise TypeError
        if date_var:
            df['date'] = date
        df_final = df_final.append(df)
        if drop_duplicates:
            df_final = df_final.drop_duplicates()
    return df_final.reset_index(drop=True)
