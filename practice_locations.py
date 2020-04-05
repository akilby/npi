import pandas as pd

from NPI_Clean import NPI, src
from functools import reduce

source_file = '/work/akilby/npi/FOIA_12312019_datefilled_clean_NPITelefill.csv'

usable_data = pd.read_csv('/work/akilby/npi/samhsa_npi_usable_data.csv')
# Still missing about 2000 people

npis = usable_data.npi.drop_duplicates()

npi = NPI(src=src, npis=npis)
npi.retrieve('locstatename')
npi.retrieve('loccityname')
npi.retrieve('loczip')
npi.retrieve('loctel')
npi.retrieve('locline1')
npi.retrieve('locline2')

dfs = [npi.locline1, npi.locline2,
       npi.loccityname, npi.locstatename,
       npi.loczip, npi.loctel]
df_merged = reduce(lambda left, right: pd.merge(left, right), dfs)
df_merged['zip'] = df_merged.ploczip.str[:5]

person_addresses = df_merged.drop(columns='month').drop_duplicates()

person_addresses = person_addresses[~(person_addresses.isnull()
                                                      .sum(axis=1) == 6)]

duplicating = ['ploctel', 'zip', 'plocstatename']
mask = person_addresses[duplicating].duplicated(keep=False)
copractice = person_addresses[mask].sort_values(duplicating)

locs = person_addresses[duplicating].drop_duplicates()
locs = locs.reset_index(drop=True).reset_index().rename(columns={'index': 'location_no'})


dates = usable_data[['npi','Date']].sort_values(['npi', 'Date'])
dates = dates.assign(month=lambda x: pd.to_datetime(x.Date).astype('datetime64[M]'))


df_merged = df_merged.assign(month=pd.to_datetime(df_merged.month)).merge(locs).sort_values(['npi', 'month'])

df_merged = df_merged.merge(dates.drop_duplicates(), on='npi')

df_merged = df_merged.assign(diff=abs(df_merged.month_x-df_merged.month_y))

df_merged = df_merged.merge(df_merged.groupby(['npi', 'Date', 'month_y'])['diff'].min().reset_index())

usable_data = usable_data.merge(df_merged[['npi', 'Date', 'location_no']].drop_duplicates(), how='left')
usable_data = usable_data.merge(locs)

usable_data.to_csv('/work/akilby/npi/samhsa_npi_usable_data_with_locs.csv', index=False)

