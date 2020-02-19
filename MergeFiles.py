import glob
import os
from pprint import pprint

import pandas as pd

USE_VAR_LIST = ['taxcode', 'locline1', 'locline2',
                'loccityname', 'locstatename', 'loczip',
                'orgname', 'loctel', 'orgnameothcode',
                'credential', 'credentialoth', 'fname',
                'fnameoth', 'gender', 'lname', 'licnum',
                'licstate', 'primtax', 'enumdate']

# Add variable for is this NPI for an organization or a person

USE_VAR_LIST_DICT = {
    'taxcode': ['Healthcare Provider Taxonomy Code_1',
                'Healthcare Provider Taxonomy Code_2',
                'Healthcare Provider Taxonomy Code_3',
                'Healthcare Provider Taxonomy Code_4',
                'Healthcare Provider Taxonomy Code_5',
                'Healthcare Provider Taxonomy Code_6',
                'Healthcare Provider Taxonomy Code_7',
                'Healthcare Provider Taxonomy Code_8',
                'Healthcare Provider Taxonomy Code_9',
                'Healthcare Provider Taxonomy Code_10',
                'Healthcare Provider Taxonomy Code_11',
                'Healthcare Provider Taxonomy Code_12',
                'Healthcare Provider Taxonomy Code_13',
                'Healthcare Provider Taxonomy Code_14',
                'Healthcare Provider Taxonomy Code_15'],
    'locline1': 'Provider First Line Business Practice Location Address',
    'locline2': 'Provider Second Line Business Practice Location Address',
    'loccityname': 'Provider Business Practice Location Address City Name',
    'locstatename': 'Provider Business Practice Location Address State Name',
    'loczip': 'Provider Business Practice Location Address Postal Code',
    'loctel': 'Provider Business Practice Location Address Telephone Number',
    'credential': 'Provider Credential Text',
    'credentialoth':  'Provider Other Credential Text',
    'fname': 'Provider First Name',
    'gender': 'Provider Gender Code',
    'lname': 'Provider Last Name (Legal Name)'
}

DTYPES = {
    'npi': 'int',
    'seq': 'int',
    'ptaxcode': 'string',
    'pprimtax': 'string',
    'ptaxgroup': 'string',
}


def get_filepaths(variable,
                  path='/work/akilby/npi/raw',
                  noisily=True):
    files = glob.glob(os.path.join(path, 'p%s*' % variable))
    file_dict = {(x.split(variable)[1].split('.')[0][:4],
                  x.split(variable)[1].split('.')[0][4:]): x
                 for x in files}
    if noisily:
        print('For variable %s, there are %s files:'
              % (variable, len(file_dict)))
        pprint(sorted(list(file_dict.keys())))
    return file_dict


def coerce_dtypes(col, orig_dtype, final_dtype):
    if orig_dtype == 'object' and final_dtype == 'string':
        new_col = col.astype('string')
        assert new_col.isna().sum() == col.isna().sum()
        return new_col
    elif orig_dtype == 'object' and final_dtype == 'int':
        new_col = col.astype(int)
        assert new_col.isna().sum() == col.isna().sum()
        assert (df.astype(int)
                  .astype(float)
                  .equals(df.astype(float)))
        return new_col
    else:
        raise Exception('havent programmed that in yet: '
                        'orig_dtype %s, final_dtype %s'
                        % (orig_dtype, final_dtype))


def check_dtypes(df):
    current_dtypes = {x: 'int' for x in df.select_dtypes('int').columns}
    for t in ['object', ['float32', 'float64'], 'datetime', 'string']:
        current_dtypes.update({x: t for x in df.select_dtypes(t).columns})
    for col in df.columns:
        if current_dtypes[col] != DTYPES[col]:
            df = df.assign(**{col: coerce_dtypes(df[col],
                                                 current_dtypes[col],
                                                 DTYPES[col])})
    return df







def concat_files_by_variable(variable, yearrange=range(2007, 2020)):
    """
    Function reads in all normal NBER NPI/NPPES files, and
    concatenates all available months by variable
    """
    df_list = []
    for (year, month), file_path in file_dict.items():
        df = (pd.read_csv(file_path) if file_path.endswith('.csv')
              else pd.read_stata(file_path))
        df = check_dtypes(df)

        convert, then check missings add up

        df['month'] = pd.to_datetime('%s-%s' % (year, month))


        df_list.append(df)
            except FileNotFoundError:
                try:
                    file_path2 = '%s.dta' % file_path_stub
                    df2 = pd.read_stata(file_path2)
                    df2['month'] = pd.to_datetime('%s-%s' % (year, month))
                    df_list.append(df2)
                    print('Combining Files: %s-%s' % (year, month))
                except FileNotFoundError:
                    print('Warning: data does not exist')
    df = pd.concat(df_list, axis=0)
    # df = df.sort_values(['npi', 'month'])
    return df.reset_index(drop=True)


def nppes_df(variable):
    df1 = concat_files_by_variable(variable)
    df2 = concat_backfiles_by_variable(variable)
    df = pd.concat([df1, df2], axis=0)


for variable in USE_VAR_LIST:
    df.to_csv('/work/akilby/npi/data/%s_nber.csv' % variable, index=False)




# FIND THE REST OF THESE
# are we able to identify organizations
['NPI',
 'orgname',
 'orgnameothcode',
 'fnameoth',
 'licnum',
 'licstate',
 'primtax',
 'Provider Enumeration Date',
 'Last Update Date']


def concat_backfiles_by_variable(variable):
    
    backfile_list = ['/work/akilby/npi/raw/npidata_20050523-20081110.csv',
                     '/work/akilby/npi/raw/npidata_20050523-20100111.csv',
                     '/work/akilby/npi/raw/npidata_20050523-20100208.csv',
                     '/work/akilby/npi/raw/npidata_20050523-20110314.csv']
                     
    use_variable = USE_VAR_LIST_DICT[variable]
    
    if isinstance(use_variable, list):
        collist = ['NPI'] + use_variable
        
        df_main = pd.DataFrame()
        for filename in backfile_list:
            df = pd.read_csv(filename, usecols=collist, low_memory=False)
            df = pd.wide_to_long(df, ["Healthcare Provider Taxonomy Code_"], i="NPI", j="seq").dropna()
            df = df.reset_index().rename(columns={'NPI': 'npi', 'Healthcare Provider Taxonomy Code_': 'ptaxcode'})
            filedate = filename.split('/')[-1].split('-')[1].replace('.csv', '')
            year, month = filedate[:4], filedate[4:6]
            df['month'] = pd.to_datetime('%s-%s' % (year, month))
            df_main = pd.concat([df_main, df], axis=0)
            
    return df_main

# Note: need to prepend 'p' to these variables

query_dataset = 'taxcode'
query_string = 'NPI=="390200000X"'
output_variables = ['locline1', 'locline2', 'loccityname', 'locstatename', 'loczip']


def query_npi(query_dataset, query_string, output_variables):
    # 1. read in appropriate dataset
    # 2. do the query on the data, get out all the NPI-months that match
    # 3. extract from each dataset for the output varables, for each NPI
    df.query(query_string)
    pass
