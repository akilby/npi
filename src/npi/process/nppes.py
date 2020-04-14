import glob
import os
import sys
from pprint import pprint

import pandas as pd

from .physician_compare import foo

# from ..constants import (DATA_DIR, DTYPES, RAW_DATA_DIR, USE_VAR_LIST_DICT,
#                          USE_VAR_LIST_DICT_REVERSE)
# from ..download.nppes import nppes_month_list

# from ..utils.utils import month_name_to_month_num


def get_filepaths_from_dissemination_zips(folder):
    '''
    Each dissemination folder contains a large / bulk data file of the format
    npidata_20050523-yearmonthday.csv, sometimes
    deep in a subdirectory. This identifies the likeliest candidate and maps
    in a dictionary to the main zip folder
    '''
    zip_paths = os.path.join(folder, 'NPPES_Data_Dissemination*')
    stub = os.path.join(folder, 'NPPES_Data_Dissemination_')
    folders = [x for x
               in glob.glob(zip_paths)
               if not x.endswith('.zip')]
    folders = [x for x in folders if 'Weekly' not in x]
    possbl = list(set(glob.glob(zip_paths + '/**/*npidata_*', recursive=True)))
    possbl = [x for x in possbl if 'Weekly' not in x]
    paths = {(x.partition(stub)[2].split('/')[0].split('_')[1],
              str(month_name_to_month_num(
                x.partition(stub)[2].split('/')[0].split('_')[0]))): x
             for x in possbl if 'eader' not in x}
    assert len(folders) == len(paths)
    return paths


def get_weekly_dissemination_zips(folder):
    '''
    Each weekly update folder contains a large / bulk data file of the format
    npidata_pfile_20200323-20200329, representing the week covered
    '''
    zip_paths = os.path.join(folder, 'NPPES_Data_Dissemination*')
    stub = os.path.join(folder, 'NPPES_Data_Dissemination_')
    folders = [x for x
               in glob.glob(zip_paths)
               if not x.endswith('.zip')]
    folders = [x for x in folders if 'Weekly' in x]
    possbl = list(set(glob.glob(zip_paths + '/**/*npidata_*', recursive=True)))
    possbl = [x for x in possbl if 'Weekly' in x]
    paths = {(x.partition(stub)[2].split('/')[0].split('_')[0],
             x.partition(stub)[2].split('/')[0].split('_')[1]): x
             for x in possbl if 'eader' not in x}
    assert len(folders) == len(paths)
    return paths


def which_weekly_dissemination_zips_are_updates(folder):
    """
    """
    last_monthly = max([pd.to_datetime(val.split('-')[1]
                                          .split('.csv')[0]
                                          .replace(' Jan 2013/', '')
                                          .replace('npidata_', ''))
                        for key, val in
                        get_filepaths_from_dissemination_zips(folder).items()])
    updates = [(x, val) for x, val
               in get_weekly_dissemination_zips(folder).items()
               if pd.to_datetime(x[1]) > last_monthly]
    return updates


def get_secondary_loc_filepaths_from_dissemination_zips(folder):
    zip_paths = os.path.join(folder, 'NPPES_Data_Dissemination*')
    stub = os.path.join(folder, 'NPPES_Data_Dissemination_')
    possbl = list(set(glob.glob(zip_paths + '/**/pl_pfile_*', recursive=True)))
    paths = {(x.partition(stub)[2].split('/')[0].split('_')[1],
              str(month_name_to_month_num(
               x.partition(stub)[2].split('/')[0].split('_')[0]))): x
             for x in possbl if 'eader' not in x}
    return paths


def get_filepaths_from_single_variable_files(variable, folder, noisily=True):
    '''
    Returns a dictionary of the path to each single variable file, for
    each month and year
    '''
    files = glob.glob(os.path.join(folder, '%s*' % variable))
    file_dict = {(x.split(variable)[1].split('.')[0][:4],
                  x.split(variable)[1].split('.')[0][4:]): x
                 for x in files}
    if noisily:
        print('For variable %s, there are %s files:'
              % (variable, len(file_dict)))
        pprint(sorted(list(file_dict.keys())))
    return file_dict


def coerce_dtypes(col, orig_dtype, final_dtype):
    '''
    Converts to destination dtype and runs some sanity checks
    to make sure no harm has been done in the conversion
    '''
    new_col = col.astype(final_dtype)

    # Checks countable nulls are maintained
    assert new_col.isna().sum() == col.isna().sum()

    if final_dtype.lower().startswith('int'):
        # This checks numeric types are actually integers and I'm not
        # asserting/enforcing rounding
        assert (new_col.astype(final_dtype)
                       .astype(float)
                       .equals(new_col.astype(float)))

    assert all(new_col.index == col.index)
    return new_col


def convert_dtypes(df):
    '''
    '''
    current_dtypes = {x: 'int' for x in df.select_dtypes('int').columns}
    for t in ['object', ['float32', 'float64'], 'datetime', 'string']:
        current_dtypes.update({x: t for x in df.select_dtypes(t).columns})
    dissem_file = (not set(current_dtypes.keys()).issubset(DTYPES.keys()))
    for col in df.columns:
        final_dtype = (DTYPES[col] if not dissem_file
                       else DTYPES[USE_VAR_LIST_DICT_REVERSE[col]])
        if (current_dtypes[col] != final_dtype and
                final_dtype not in current_dtypes[col]):
            try:
                df = df.assign(**{col: coerce_dtypes(df[col],
                                                     current_dtypes[col],
                                                     final_dtype)})
            except ValueError as err:
                if final_dtype == 'string':
                    newcol = coerce_dtypes(df[col], current_dtypes[col], 'str')
                    newcol = coerce_dtypes(newcol, 'str', 'string')
                else:
                    raise ValueError("{0}".format(err))
    return df


def column_details(variable, dissem_file, dta_file):
    '''
    Generates column list to get from the raw data; dissem files
    have long string names and are wide, whereas NBER files have
    short names and are long
    '''
    diss_var = USE_VAR_LIST_DICT[variable]
    multi = True if isinstance(diss_var, list) else False
    tvar = ['npi', 'seq']
    if not dissem_file:
        if multi:
            if str.isupper(variable) and not dta_file:
                def collist(col): return col.upper() == variable or col in tvar
            elif str.isupper(variable) and dta_file:
                collist = tvar + [variable.lower()]
            else:
                collist = tvar + [variable]
        else:
            collist = ['npi', variable]
        d_use = {} if not variable == 'ploczip' else {'ploczip': str}
    else:
        diss_vars = diss_var if multi else [diss_var]
        collist = (['NPI'] + diss_var if multi else ['NPI'] + [diss_var])
        d_use = {x: object for x in diss_vars if DTYPES[variable] == 'string'}
    return collist, d_use


def locate_file(folder, year, month, variable):
    '''
    '''
    paths1 = get_filepaths_from_single_variable_files(variable, folder, False)
    if not variable.startswith('ploc2'):
        paths2 = get_filepaths_from_dissemination_zips(folder)
    else:
        paths2 = get_secondary_loc_filepaths_from_dissemination_zips(folder)
    try:
        return paths1[(year, month)]
    except KeyError:
        try:
            return paths2[(year, month)]
        except KeyError:
            return None


def read_and_process_df(folder, year, month, variable):
    '''
    Locates and reads in year-month-variable df from disk,
    checks and converts dtypes, makes consistent variable names,
    and adds a standardized month column
    '''
    file_path = locate_file(folder, '%s' % year, '%s' % month, variable)
    if file_path:
        df = process_filepath_to_df(file_path, variable)
        df['month'] = pd.to_datetime('%s-%s' % (year, month))
        return df


def read_and_process_weekly_updates(folder, variable):
    """
    """
    filepaths = which_weekly_dissemination_zips_are_updates(folder)
    updates = pd.concat(
        [process_filepath_to_df(f[1], variable).assign(
            week=pd.to_datetime(f[0][0]))
         for f in filepaths])
    updates['month'] = (pd.to_datetime(updates.week.dt.year.astype(str)
                        + '-'
                        + updates.week.dt.month.astype(str) + '-' + '1'))
    updates = (updates.dropna()
                      .groupby(['npi', 'month'])
                      .max()
                      .reset_index()
                      .merge(updates)
                      .drop(columns='week'))
    return updates


def process_filepath_to_df(file_path, variable):
    """
    """
    is_dissem_file = len(file_path.split('/')) > 6
    is_dta_file = os.path.splitext(file_path)[1] == '.dta'
    collist, d_use = column_details(variable, is_dissem_file, is_dta_file)
    df = (pd.read_csv(file_path, usecols=collist, dtype=d_use)
          if file_path.endswith('.csv')
          else pd.read_stata(file_path, columns=collist))
    if (not is_dissem_file
            and variable not in df.columns
            and variable.lower() in df.columns):
        df = df.rename(columns={variable.lower(): variable})
    df = convert_dtypes(df)
    df = reformat(df, variable, is_dissem_file)
    return df


def reformat(df, variable, is_dissem_file):
    '''
    '''
    multi = True if isinstance(USE_VAR_LIST_DICT[variable], list) else False
    if is_dissem_file and multi:
        stb = list(set([x.split('_')[0] for x in USE_VAR_LIST_DICT[variable]]))
        assert len(stb) == 1
        stb = stb[0] + '_'
        df = pd.wide_to_long(df, [stb], i="NPI", j="seq").dropna()
        df = df.reset_index().rename(columns={'NPI': 'npi', stb: variable})
    elif is_dissem_file:
        df = df.rename(columns={x: USE_VAR_LIST_DICT_REVERSE[x]
                                for x in df.columns})
    return df


def process_variable(folder, variable, searchlist, final_weekly_updates=True):
    '''
    '''
    # searchlist = [x for x in searchlist if x != (2011, 3)]
    df_list = []
    for (year, month) in searchlist:
        print(year, month)
        if variable == "PTAXGROUP":
            try:
                df = read_and_process_df(folder, year, month, variable)
            except ValueError as err:
                assert year < 2012
        else:
            df = read_and_process_df(folder, year, month, variable)
        df_list.append(df)
    df = pd.concat(df_list, axis=0) if df_list else None
    if df_list and final_weekly_updates:
        u = read_and_process_weekly_updates(folder, variable)
        df = df.merge(u, on=['npi', 'month'], how='outer', indicator=True)
        df.loc[df._merge == "right_only",
               '%s_x' % variable] = df['%s_y' % variable]
        df.loc[df._merge == "both", '%s_x' % variable] = df['%s_y' % variable]
        df = (df.drop(columns=['_merge', '%s_y' % variable])
                .rename(columns={'%s_x' % variable: variable}))
        assert df[['npi', 'month']].drop_duplicates().shape[0] == df.shape[0]
    return df


def main_process_variable(variable, update):
    # Should figure out NPPES_Data_Dissemination_March_2011 because it's weird;
    # deleting for now
    if not update:
        searchlist = [x for x in nppes_month_list() if x != (2011, 3)]
        df = process_variable(RAW_DATA_DIR, variable, searchlist)
        df.to_csv(os.path.join(DATA_DIR, '%s.csv' % variable),
                  index=False)
    else:
        print('Updating:')
        df = pd.read_csv(os.path.join(DATA_DIR, '%s.csv' % variable))
        df['month'] = pd.to_datetime(df.month)
        last_month = max(list(df.month.value_counts().index))
        searchlist = [x for x in nppes_month_list() if
                      (pd.to_datetime('%s-%s-01' % (x[0], x[1]))
                       >= pd.to_datetime(last_month) - pd.DateOffset(months=6))
                      ]
        print('updating (destroying and remaking) months->', searchlist)
        if searchlist != [] or update == 'Force':
            df2 = process_variable(RAW_DATA_DIR, variable, searchlist)
            # df = pd.concat([df, df2], axis=0)
            dim1 = df.loc[df.month >= df2.month.min()].shape[0]
            dim2 = df.loc[df.month >= df2.month.min()].merge(
                df2, on=['npi', 'month']).shape[0]
            assert dim1 == dim2
            df = df.loc[df.month < df2.month.min()].append(df2)
            assert (df[['npi', 'month']].drop_duplicates().shape[0]
                    == df.shape[0])
            df = df.query('month != "2011-03-01"')
            df.to_csv(os.path.join(DATA_DIR, '%s.csv' % variable),
                      index=False)


def main():
    variable = sys.argv[1]
    update = sys.argv[2] if len(sys.argv) > 2 else False
    update = update if (update == 'True' or update == 'Force') else False
    main_process_variable(variable, update)


if __name__ == '__main__':
    print('lol')
    print(foo)
    # main()
