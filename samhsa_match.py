import os
import re
import time

import pandas as pd
from NPI_Clean import NPI, expand_names_in_sensible_ways, src

unmatched = pd.read_csv('/work/akilby/npi/raw_samhsa/check.csv')
source_file = '/work/akilby/npi/FOIA_12312019_datefilled_clean_NPITelefill.csv'

npi = NPI(src=src)
npi.retrieve('fullnames')
npi.retrieve('expanded_fullnames')
npi.retrieve('credentials')
npi.retrieve('taxcode')
npi.retrieve('locstatename')
npi.retrieve('loczip')

new = (npi.expanded_fullnames
          .merge((npi.taxcode
                     .dropna()
                     .assign(practype=lambda x:
                             x.cat.str.replace(' Student', ''))
                     .drop(columns=['cat', 'ptaxcode'])
                     .drop_duplicates()), how='left')
          .merge((npi.locstatename[['npi', 'plocstatename']]
                     .drop_duplicates()), how='left'))

q = 'pcredential_stripped=="MD" or pcredential_stripped=="DO"'

new = (new.append((new.merge(new.loc[new.practype == "MD/DO"][['npi']]
                                .drop_duplicates()
                                .merge(npi.credentials
                                          .query(q)[['npi']]
                                          .drop_duplicates(),
                                       how='outer', indicator=True)
                                .query('_merge=="right_only"')
                                .drop(columns='_merge'))
                      .fillna(value={'practype': 'MD/DO'})
                      .append((new.merge(
                        new.loc[new.practype == "MD/DO"][['npi']]
                           .drop_duplicates()
                           .merge(npi.credentials.query(q)[['npi']]
                                                 .drop_duplicates(),
                                  how='outer', indicator=True)
                           .query('_merge=="right_only"')
                           .drop(columns='_merge'))
                                  .assign(practype='MD/DO')))
                      .drop_duplicates()))
          .drop_duplicates())


class SAMHSA(object):
    def __init__(self, src):
        self.source_file = src
        self.samhsa = self.make_samhsa_id(pd.read_csv(src, low_memory=False))

    def make_samhsa_id(self, samhsa, idvars=['NameFull', 'DateLastCertified',
                                             'PractitionerType']):
        """
        SAMHSA files do not have an identifier. Make an arbitrary one for
        tracking throughout the class. Note: this will not be stable across
        different versions of the SAMHSA data
        """
        for idvar in idvars:
            samhsa[idvar] = samhsa[idvar].str.upper()
        ids = (samhsa[idvars].drop_duplicates()
                             .reset_index(drop=True)
                             .reset_index()
                             .rename(columns=dict(index='samhsa_id')))
        return samhsa.merge(ids)

    def get_names(self, namecol='NameFull'):
        names = (self.samhsa[[namecol, 'samhsa_id']]
                     .drop_duplicates()
                     .sort_values('samhsa_id')
                     .reset_index(drop=True))
        # remove credentials from end
        badl = credential_suffixes()
        names = remove_suffixes(names, badl)
        names[namecol] = remove_mi_periods(names[namecol])
        while not remove_suffixes(names.copy(), badl).equals(names.copy()):
            names = remove_suffixes(names, badl)

        # split into first, middle, last -- note that a maiden name is middle
        # name here, could maybe use that later

        names_long = (names[namecol].str.replace('    ', ' ')
                                    .str.replace('   ', ' ')
                                    .str.replace('  ', ' ')
                                    .str.split(' ', expand=True)
                                    .stack()
                                    .reset_index())

        firstnames = names_long.groupby('level_0').first().reset_index()
        lastnames = names_long.groupby('level_0').last().reset_index()
        middlenames = (names_long.merge(firstnames, how='left', indicator=True)
                                 .query('_merge=="left_only"')
                                 .drop(columns='_merge')
                                 .merge(lastnames, how='left', indicator=True)
                                 .query('_merge=="left_only"')
                                 .drop(columns='_merge')
                                 .set_index(['level_0', 'level_1']).unstack(1))
        middlenames = middlenames.fillna('').agg(' '.join, axis=1).str.strip()

        allnames = (firstnames.merge(pd.DataFrame(middlenames),
                                     left_index=True, right_index=True,
                                     how='outer')
                              .merge(lastnames,
                                     left_index=True, right_index=True)
                              .fillna('')
                              .drop(columns=['level_1_x', 'level_1_y'])
                              .rename(columns={'0_x': 'firstname', '0_y':
                                               'middlename', 0: 'lastname'}))
        names = pd.concat([allnames, names], axis=1)

        cols = ['firstname', 'middlename', 'lastname', 'samhsa_id']
        newnames = expand_names_in_sensible_ways(
            names[cols].reset_index(drop=True),
            'samhsa_id', 'firstname', 'middlename', 'lastname')
        newnames = (newnames.append(names[cols + ['NameFull']].rename(
                                columns={'NameFull': 'name'}))
                            .drop_duplicates())

        self.names = newnames.merge(names[['samhsa_id', 'Credential String']])


s = SAMHSA(src)
sam = s.names.merge(s.samhsa[['PractitionerType', 'State', 'samhsa_id']].drop_duplicates())
samhsa_matches = sam.drop(columns=['firstname', 'middlename', 'lastname', 'Credential String']).merge(new.drop(columns=['pfname','pmname','plname']), left_on=['name','PractitionerType','State'], right_on=['name','practype','plocstatename'])
samhsa_matches = samhsa_matches.merge(samhsa_matches[['samhsa_id', 'npi']].drop_duplicates().groupby('samhsa_id').count().reset_index().query('npi>1').drop(columns='npi'), on='samhsa_id', indicator=True, how='outer').query('_merge=="left_only"').drop(columns=['_merge'])
samhsa_matches = samhsa_matches[~samhsa_matches.npi.isin(samhsa_matches[['samhsa_id', 'npi']].drop_duplicates().groupby('npi').count().query('samhsa_id>1').reset_index().npi)]

matches1 = samhsa_matches[['samhsa_id', 'npi']].drop_duplicates()
remainders1 = sam.merge(samhsa_matches[['samhsa_id']].drop_duplicates(), how='outer', indicator=True).query('_merge!="both"').drop(columns='_merge')

remainders1 = remainders1.query('middlename!=""').merge(new.query('pmname!=""'), left_on=['name','PractitionerType','State'], right_on=['name','practype','plocstatename']).sort_values('samhsa_id')
matches2 = remainders1[~remainders1.samhsa_id.isin(remainders1[['samhsa_id', 'npi']].drop_duplicates().groupby('samhsa_id').count().query('npi>1').reset_index().samhsa_id)]
matches2 = matches2[['samhsa_id', 'npi']].drop_duplicates()

matches = matches1.append(matches2)

dups = matches[['npi']][matches[['npi']].duplicated()]
dd = s.samhsa.drop(columns='npi').merge(matches[matches.npi.isin(dups.npi)], on='samhsa_id').sort_values('npi').drop(columns=['Unnamed: 0', 'LocatorWebsiteConsent','First name', 'Last name','NPI state','Date', 'Telephone', 'statecode', 'geoid', 'zcta5', 'zip', 'CurrentPatientLimit', 'NumberPatientsCertifiedFor', 'Index', 'DateGranted', 'Street2'])
dd[['County']] = dd.County.str.upper()
dd[['City']] = dd.City.str.upper()

a1 = dd[~dd.npi.isin(dd[['npi','PractitionerType','Phone']].drop_duplicates()[dd[['npi','PractitionerType','Phone']].drop_duplicates().npi.duplicated()].npi)][['samhsa_id', 'npi']]
a2 = dd[~dd.npi.isin(dd[['npi','PractitionerType','County']].drop_duplicates()[dd[['npi','PractitionerType','County']].drop_duplicates().npi.duplicated()].npi)][['samhsa_id', 'npi']]
a3 = dd[~dd.npi.isin(dd[['npi','PractitionerType','City', 'State']].drop_duplicates()[dd[['npi','PractitionerType','City', 'State']].drop_duplicates().npi.duplicated()].npi)][['samhsa_id', 'npi']]
fine = a1.append(a2).append(a3).drop_duplicates()
dups[~dups.npi.isin(fine.npi)]

matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]
remainders2 = sam.merge(matches, how='outer', indicator=True).query('_merge!="both"').drop(columns=['_merge', 'npi'])

zips = npi.loczip[['npi', 'ploczip']].drop_duplicates()
zips['zip']=zips.ploczip.str[:5]
zips = zips[['npi','zip']].drop_duplicates()
new2 = new.drop(columns='plocstatename').drop_duplicates().merge(zips)

sam2 = s.names.merge(s.samhsa[['PractitionerType', 'Zip', 'samhsa_id']].drop_duplicates())
sam2['zip'] = sam2.Zip.str[:5]
sam2 = sam2.drop(columns="Zip").drop_duplicates()
sam2 = sam2.merge(remainders2[['samhsa_id']].drop_duplicates())
new_matches = sam2.drop(columns=['firstname', 'middlename', 'lastname', 'Credential String']).merge(new2.drop(columns=['pfname','pmname','plname']), left_on=['name','PractitionerType','zip'], right_on=['name','practype','zip'])
matches = matches.append(new_matches[['samhsa_id', 'npi']].drop_duplicates())
dups = matches[['npi']][matches[['npi']].duplicated()]
matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]

remainders3 = sam.merge(matches, how='outer', indicator=True).query('_merge!="both"').drop(columns=['_merge', 'npi'])


new3 = new2[['npi','name','practype']].drop_duplicates()
match4 = remainders3[['name','PractitionerType','samhsa_id']].drop_duplicates().merge(new3, left_on=['name','PractitionerType'], right_on=['name','practype'])
match4 = match4[['samhsa_id', 'npi']].drop_duplicates()
match4 = match4[~match4.samhsa_id.isin(match4[match4.samhsa_id.duplicated()].samhsa_id.drop_duplicates())]
matches = matches.append(match4[['samhsa_id', 'npi']].drop_duplicates())
dups = matches[['npi']][matches[['npi']].duplicated()]
matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]
remainders4 = sam.merge(matches, how='outer', indicator=True).query('_merge!="both"').drop(columns=['_merge', 'npi'])


new4 = new2.query('pmname!=""')[['npi','name','practype']].drop_duplicates()
match5 = remainders4.query('middlename!=""')[['name','PractitionerType','samhsa_id']].drop_duplicates().merge(new4, left_on=['name','PractitionerType'], right_on=['name','practype'])
match5 = match5[['samhsa_id', 'npi']].drop_duplicates()
match5 = match5[~match5.samhsa_id.isin(match5[match5.samhsa_id.duplicated()].samhsa_id.drop_duplicates())]
matches = matches.append(match5[['samhsa_id', 'npi']].drop_duplicates())
dups = matches[['npi']][matches[['npi']].duplicated()]
matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]
remainders5 = sam.merge(matches, how='outer', indicator=True).query('_merge!="both"').drop(columns=['_merge', 'npi'])


#these all actually look correct
matches[matches.samhsa_id.isin(matches[matches.samhsa_id.duplicated()].samhsa_id)].head(60)


# check for special characters
"ELIZABETH MARIE O DAIR" == "ELIZABETH MARIE O'DAIR"
"STEPHAINE JULIA HUCKER" == "STEPHAINE HUCKER"
"ELIZABETH M LIDSTONE JAYANATH" == "ELIZABETH  MAUDE LIDSTONE-JAYANATH"

# samhsa = samhsa.drop(columns=['Unnamed: 0', 'LocatorWebsiteConsent',
#                               'First name', 'Last name', 'npi', 'NPI state',
#                               'Date', 'CurrentPatientLimit', 'Telephone',
#                               'zip', 'zcta5', 'geoid', 'Index',
#                               'NumberPatientsCertifiedFor', 'statecode'])

# mindate = (samhsa[['samhsa_id', 'DateGranted']].dropna()
#                                                .groupby('samhsa_id')
#                                                .min().reset_index())
# 
# samhsa = (samhsa.fillna('')
#                 .groupby(['NameFull', 'samhsa_id', 'PractitionerType',
#                           'DateLastCertified', 'Street1', 'Street2',
#                           'City', 'State', 'Zip', 'County', 'Phone'])
#                 .agg({'WaiverType': max})
#                 .reset_index())
# 
# samhsa = samhsa.merge(mindate, how='left')


def credential_suffixes():
    badl = ['(JR.)', '(M.D.)', '(RET.)', 'M., PH', 'M. D.', 'M.D.,',
            'M.D., PHD, DABA,',
            'M .D.', 'M.D.', 'MD', 'NP', 'D.O.', 'PA', 'DO', '.D.', 'MPH',
            'PH.D.', 'JR', 'M.D', 'PHD', 'P.A.', 'FNP', 'PA-C', 'M.P.H.',
            'N.P.', 'III', 'SR.', 'D.', '.D', 'FASAM', 'JR.', 'MS',
            'D', 'FAAFP', 'SR', 'D.O', 'CNS', 'F.A.S.A.M.', 'FNP-BC', 'P.C.',
            'MBA', 'M.S.', 'PH.D', 'FACP', 'M.P.H', 'CNM',
            'NP-C', 'MR.', 'MDIV', 'FACEP', 'PLLC', 'M.A.', 'LLC', 'MR',
            'DNP', 'PHD.', 'FNP-C', 'MD.', 'CNP', 'J.D.', 'IV', 'F.A.P.A.',
            'DR.', 'M.D,', 'DABPM', 'M,D.', 'MS.', 'FACOOG', 'APRN']

    badl2 = (pd.read_csv('/work/akilby/npi/stubs_rev.csv')
               .rename(columns={'Unnamed: 2': 'flag'})
               .query('flag==1')['Unnamed: 0']
               .tolist())

    badl = badl + badl2
    return badl


def remove_suffixes(samhsa, badl):
    name_col = samhsa.columns.tolist()[0]
    for b in badl:
        samhsa.loc[samhsa[name_col].apply(lambda x: x.endswith(' ' + b)),
                   'Credential String'] = (samhsa[name_col].apply(
                    lambda x: x[len(x)-len(b):]))
        samhsa[name_col] = (samhsa[name_col].apply(
            lambda x: x[:len(x)-len(b)] if x.endswith(' ' + b) else x))
        samhsa[name_col] = samhsa[name_col].str.strip()
        samhsa[name_col] = (samhsa[name_col].apply(
            lambda x: x[:-1] if x.endswith(',') else x))
    return samhsa


def remove_mi_periods(col):
    return col.apply(lambda x: re.sub('(.*\s[A-Z]?)(\.)(\s)', r'\1\3', x))



# name_matches = expanded_fullnames[['name','npi']].merge(expanded_names[['name','samhsa_id']])
# 
# 
# # taxcode.merge(credentials, how='outer')
# 
# samhsa.merge(npi.fullnames[['npi','name']], left_on='NameFull', right_on='name').merge(npi.credentials, how='left', on='npi').merge(npi.taxcode, how='left', on='npi')


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
