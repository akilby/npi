import glob
import os
import sys
from pprint import pprint

import pandas as pd
from constants import (DATA_DIR, DTYPES, RAW_DATA_DIR, USE_VAR_LIST_DICT,
                       USE_VAR_LIST_DICT_REVERSE)
from NPI_Data_Download import nppes_month_list
from utils import month_name_to_month_num


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
    possbl = list(set(glob.glob(zip_paths + '/**/*npidata_*', recursive=True)))
    paths = {(x.partition(stub)[2].split('/')[0].split('_')[1],
              str(month_name_to_month_num(
                x.partition(stub)[2].split('/')[0].split('_')[0]))): x
             for x in possbl if 'eader' not in x}
    assert len(folders) == len(paths)
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
            df = df.assign(**{col: coerce_dtypes(df[col],
                                                 current_dtypes[col],
                                                 final_dtype)})
    return df


def column_details(variable, dissem_file):
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
            if str.isupper(variable):
                def collist(col): return col.upper() == variable or col in tvar
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
    paths2 = get_filepaths_from_dissemination_zips(folder)
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
        is_dissem_file = len(file_path.split('/')) > 6
        collist, d_use = column_details(variable, is_dissem_file)
        df = (pd.read_csv(file_path, usecols=collist, dtype=d_use)
              if file_path.endswith('.csv')
              else pd.read_stata(file_path, columns=collist))
        df = convert_dtypes(df)
        df = reformat(df, variable, is_dissem_file)
        df['month'] = pd.to_datetime('%s-%s' % (year, month))
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


def process_variable(folder, variable):
    '''
    '''
    searchlist = nppes_month_list()
    # Should delete NPPES_Data_Dissemination_March_2011 because it's weird
    # searchlist = [x for x in searchlist if x != (2011, 3)]
    df_list = []
    for (year, month) in searchlist:
        print(year, month)
        df = read_and_process_df(folder, year, month, variable)
        df_list.append(df)
    return pd.concat(df_list, axis=0)


def main():
    variable = sys.argv[1]
    df = process_variable(RAW_DATA_DIR, variable)
    df.to_csv(os.path.join(DATA_DIR, '%s.csv' % variable),
              index=False)


if __name__ == '__main__':
    main()


# Note: need to prepend 'p' to these variables

# query_dataset = 'taxcode'
# query_string = 'NPI=="390200000X"'
# output_variables = ['locline1', 'locline2', 'loccityname',
#                     'locstatename', 'loczip']


# def query_npi(query_dataset, query_string, output_variables):
#     # 1. read in appropriate dataset
#     # 2. do the query on the data, get out all the NPI-months that match
#     # 3. extract from each dataset for the output varables, for each NPI
#     df.query(query_string)
#     pass
