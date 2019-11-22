import pandas as pd

USE_VAR_LIST = ['taxcode', 'locline1', 'locline2',
                'loccityname', 'locstatename', 'loczip']


def concat_files_by_variable(variable):
    """
    Function reads in all normal NBER NPI/NPPES files, and
    concatenates all available months by variable
    """
    df_list = []
    for year in range(2007, 2020):
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
    df = df.sort_values(['npi', 'month'])
    return df.reset_index(drop=True)


for variable in USE_VAR_LIST:
    df = concat_files_by_variable(variable)
    df.to_stata('/work/akilby/npi/data/%s_nber.dta' % variable)
