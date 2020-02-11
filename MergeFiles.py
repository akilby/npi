import pandas as pd

USE_VAR_LIST = ['taxcode', 'locline1', 'locline2', 'loccityname', 'locstatename', 'loczip', 'orgname', 'loctel', 'orgnameothcode', 'credential', 'credentialoth', 'fname', 'fnameoth', 'gender', 'lname', 'licnum', 'licstate', 'primtax']


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
    df = concat_files_by_variable(variable)
    df.to_csv('/work/akilby/npi/data/%s_nber.csv' % variable, index=False)


def concat_backfiles_by_variable(variable):

    backfile_list = ['npidata_20050523-20081110.csv',
                     'npidata_20050523-20100111.csv',
                     'npidata_20050523-20100208.csv',
                     'npidata_20050523-20110314.csv']

    Nov2008 = pd.read_csv('/work/akilby/npi/raw/npidata_20050523-20081110.csv',
                          usecols=['NPI', 'Provider First Line Business Practice Location Address'])
    Nov2008 = Nov2008.rename(columns={'NPI': 'npi', 'Provider First Line Business Practice Location Address': 'plocline1'})
    Nov2008['month'] = pd.to_datetime('2008-11')