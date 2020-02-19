import pandas as pd

USE_VAR_LIST = ['enumdate', 'taxcode', 'locline1', 'locline2', 'loccityname', 'locstatename', 'loczip', 'orgname', 'loctel', 'orgnameothcode', 'credential', 'credentialoth', 'fname', 'fnameoth', 'gender', 'lname', 'licnum', 'licstate', 'primtax']


def concat_files_by_variable(variable, yearrange=range(2007, 2020)):
    """
    Function reads in all normal NBER NPI/NPPES files, and
    concatenates all available months by variable
    """
    df_list = []
    for year in yearrange:
        for month in range(1, 13):
            file_path_stub = ('/work/akilby/npi/raw/p%s%s%s'
                              % (variable, year, month))
            try:
                file_path = '%s.csv' % file_path_stub
                df = pd.read_csv(file_path)
                df['month'] = pd.to_datetime('%s-%s' % (year, month))
                df_list.append(df)
                print('Combining Files: %s-%s' % (year, month))
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


for variable in USE_VAR_LIST:
    df1 = concat_files_by_variable(variable)
    df2 = concat_backfiles_by_variable(variable)
    df = pd.concat([df1, df2], axis=0)
    df.to_csv('/work/akilby/npi/data/%s_nber.csv' % variable, index=False)


USE_VAR_LIST_DICT = {'taxcode': ['Healthcare Provider Taxonomy Code_1',
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
                     'lname': 'Provider Last Name (Legal Name)'}


# FIND THE REST OF THESE
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
            df = df.reset_index().rename(columns={'NPI': 'npi', 'Healthcare Provider Taxonomy Code_': 'taxcode'})
            filedate = filename.split('/')[-1].split('-')[1].replace('.csv', '')
            year, month = filedate[:4], filedate[4:6]
            df['month'] = pd.to_datetime('%s-%s' % (year, month))
            df_main = pd.concat([df_main, df], axis=0)
            
    return df_main


query_dataset = 'taxcode'
query_string = 'NPI=="390200000X"'
output_variables = ['locline1', 'locline2', 'loccityname', 'locstatename', 'loczip']


def query_npi(query_dataset, query_string, output_variables):
    # 1. read in appropriate dataset
    # 2. do the query on the data, get out all the NPI-months that match
    # 3. extract from each dataset for the output varables, for each NPI
    df.query(query_string)
    pass