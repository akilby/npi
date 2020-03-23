import os
import re
import time

import pandas as pd

unmatched = pd.read_csv('/work/akilby/npi/raw_samhsa/check.csv')
samhsa = pd.read_csv(
    '/work/akilby/npi/FOIA_12312019_datefilled_clean_NPITelefill.csv',
    low_memory=False)
samhsa = samhsa.drop(columns=['Unnamed: 0', 'LocatorWebsiteConsent',
                              'First name', 'Last name', 'npi', 'NPI state',
                              'Date', 'CurrentPatientLimit', 'Telephone',
                              'zip', 'zcta5', 'geoid', 'Index',
                              'NumberPatientsCertifiedFor'])

npi = NPI(src=src)
npi.retrieve('fullnames')
npi.retrieve('credentials')
npi.retrieve('taxcode')
npi.retrieve('locstatename')


samhsa = pd.read_csv(
    '/work/akilby/npi/FOIA_12312019_datefilled_clean_NPITelefill.csv',
    low_memory=False)
samhsa = samhsa.drop(columns=['Unnamed: 0', 'LocatorWebsiteConsent',
                              'First name', 'Last name', 'npi', 'NPI state',
                              'Date', 'CurrentPatientLimit', 'Telephone',
                              'zip', 'zcta5', 'geoid', 'Index',
                              'NumberPatientsCertifiedFor', 'statecode'])
samhsa['NameFull'] = samhsa.NameFull.str.upper()

uvars = ['NameFull', 'PractitionerType', 'DateLastCertified', 'Street1',
         'City', 'State', 'Zip', 'County', 'Phone']

mindate = (samhsa[uvars + ['DateGranted']].dropna()
                                          .groupby(uvars).min().reset_index())

samhsa = (samhsa.fillna('')
                .groupby(['NameFull', 'PractitionerType',
                          'DateLastCertified', 'Street1', 'Street2',
                          'City', 'State', 'Zip', 'County', 'Phone'])
                .agg({'WaiverType': max})
                .reset_index())

samhsa = samhsa.merge(mindate, how='left')

badl = ['(JR.)', '(M.D.)', '(RET.)', 'M., PH', 'M. D.', 'M.D.,',
        'M.D., PHD, DABA,',
        'M .D.', 'M.D.', 'MD', 'NP', 'D.O.', 'PA', 'DO', '.D.', 'MPH',
        'PH.D.', 'JR', 'M.D', 'PHD', 'P.A.', 'FNP', 'PA-C', 'M.P.H.',
        'N.P.', 'III', 'SR.', 'D.', '.D', 'FASAM', 'JR.', 'MS',
        'D', 'FAAFP', 'SR', 'D.O', 'CNS', 'F.A.S.A.M.', 'FNP-BC', 'P.C.',
        'MBA', 'M.S.', 'PH.D', 'FACP', 'M.P.H', 'CNM',
        'NP-C', 'MR.', 'MDIV', 'FACEP', 'PLLC', 'M.A.', 'LLC', 'MR',
        'DNP', 'PHD.', 'FNP-C', 'MD.', 'CNP', 'J.D.', 'IV', 'F.A.P.A.',
        'DR.', 'M.D,', 'DABPM', 'M,D.', 'MS.', 'FACOOG']

badl2 = (pd.read_csv('/work/akilby/npi/stubs_rev.csv')
           .rename(columns={'Unnamed: 2': 'flag'})
           .query('flag==1')['Unnamed: 0']
           .tolist())

badl = badl + badl2


def remove_suffixes(samhsa, badl):
    for b in badl:
        samhsa.loc[samhsa.NameFull.apply(lambda x: x.endswith(' ' + b)),
                   'Credential String'] = (samhsa.NameFull
                                                 .apply(lambda x:
                                                        x[len(x)-len(b):]))
        samhsa['NameFull'] = (samhsa.NameFull
                                    .apply(lambda x: x[:len(x)-len(b)]
                                           if x.endswith(' ' + b) else x))
        samhsa['NameFull'] = samhsa['NameFull'].str.strip()
        samhsa['NameFull'] = (samhsa.NameFull
                                    .apply(lambda x: x[:-1]
                                           if x.endswith(',') else x))
    return samhsa


samhsa = remove_suffixes(samhsa, badl)
samhsa['NameFull'] = samhsa.NameFull.apply(
    lambda x: re.sub('(.*\s[A-Z]?)(\.)(\s)', r'\1\3', x))


samhsa = remove_suffixes(samhsa, badl)
assert remove_suffixes(samhsa.copy(), badl).equals(samhsa.copy())


df.loc[df.pmname=='', 'name']=df.pfname + ' ' + df.plname 
df.loc[df.pmname!='', 'name']=df.pfname + ' ' + df.pmname + ' ' + df.plname 
# 
# taxcode.merge(credentials, how='outer')


def check_in(it, li):
    return it in li


def replace_end(namestr, stub):
    return (namestr.replace(stub, '').strip() if namestr.endswith(stub)
            else namestr.strip())


# for b in badl:
#     df_samhsa['NameFull'] = df_samhsa.NameFull.apply(lambda x: replace_end(x, b))


def npi_names_to_samhsa_matches(firstname, lastname, df_samhsa):
    u = df_samhsa[df_samhsa.NameFull.str.contains(lastname)]
    u = u[u.NameFull.str.split(lastname).str[0].str.contains(firstname)]
    if not u.empty:
        u = u.reset_index(drop=True)
        u['splitli'] = u.NameFull.str.split(' ')
        u = u[u.splitli.apply(lambda x: check_in(firstname, x))]
        u = u[u.splitli.apply(lambda x: check_in(lastname, x))]
    return u


def state_npis(stateabbrev, states, names):
    npis = states.query(
        'plocstatename=="%s"' % stateabbrev).npi.drop_duplicates()
    return names.merge(npis)


def npi_names_sahmsa_matches_statebatch(stateabbrev):
    s = time.time()
    outcome_df, fn, ln = pd.DataFrame(), [], []
    searchdf = df_samhsa.query('State=="%s"' % stateabbrev)
    use_df = state_npis(stateabbrev, states, df)
    print('number of rows: %s' % use_df.shape[0])
    for i, row in use_df.iterrows():
        if round(i/10000) == i/10000:
            print(i)
        npi = row['npi']
        lastname = row['plname']
        firstname = row['pfname']
        try:
            o = npi_names_to_samhsa_matches(firstname, lastname, searchdf)
        except re.error:
            fn.append(firstname)
            ln.append(lastname)
        if not o.empty:
            o['npi'] = npi
            o['pfname'] = firstname
            o['plname'] = lastname
            outcome_df = outcome_df.append(o)
    print('time:', time.time() - s)
    return outcome_df, fn, ln


# outcome_df = pd.read_csv('/home/akilby/outcome_df_samhsa_match.csv')
# 
# outcome_df.drop(columns=['Unnamed: 0'], inplace=True)
# 
# merged = outcome_df.merge(locstatename, left_on=['npi', 'State'], right_on=['npi', 'plocstatename'])
# merged['diff'] = pd.to_datetime(merged['DateLastCertified']) - pd.to_datetime(merged['month'])
# merged = merged.sort_values(['NameFull', 'State', 'DateLastCertified', 'npi', 'diff'])
# merged['diff2'] = abs(merged['diff'])
# 
# possibles = merged[['NameFull', 'State', 'DateLastCertified', 'npi', 'pfname', 'plname', 'diff2']].groupby(['NameFull', 'State', 'DateLastCertified', 'npi', 'pfname', 'plname']).min()
# possibles = possibles.merge(possibles.groupby(level=[0, 1, 2]).count(), left_index=True, right_index=True)
