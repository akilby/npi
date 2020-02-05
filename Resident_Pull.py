import pandas as pd

df_list = []
for year in range(2007, 2020):
    for month in range(1, 13):
        file_path_stub = ('/work/akilby/npi/raw/ptaxcode%s%s' % (year, month))
        try:
            file_path = '%s.csv' % file_path_stub
            df = pd.read_csv(file_path)
            df['month'] = pd.to_datetime('%s-%s' % (year, month))
            df = df.query('ptaxcode=="390200000X"')
            df_list.append(df)
            print('Combining Files: %s-%s' % (year, month))
        except FileNotFoundError:
            print('Warning: data does not exist')
            try:
                file_path2 = '%s.dta' % file_path_stub
                df2 = pd.read_stata(file_path2)
                df2['month'] = pd.to_datetime('%s-%s' % (year, month))
                df2 = df2.query('ptaxcode=="390200000X"')
                df_list.append(df2)
                print('Combining Files: %s-%s' % (year, month))
            except FileNotFoundError:
                print('Warning: data does not exist')
df = pd.concat(df_list, axis=0)
header = ["npi", "month"]
df.to_csv('/work/akilby/npi/data/residents_nber.csv', columns=header, index=False)