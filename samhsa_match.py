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
src = '/work/akilby/npi/data/'


class NPI(object):
    def __init__(self, src):
        self.src = src
        self.get_entity()

    def retrieve(self, thing):
        getattr(self, f'get_{thing}')()

    def get_entity(self):
        if hasattr(self, 'entity'):
            return
        src = self.src
        entity = pd.read_csv(os.path.join(src, 'entity.csv'))
        entity = entity.dropna()
        entity['entity'] = entity.entity.astype("int")
        self.entity = entity[['npi', 'entity']].drop_duplicates()

    def get_name(self, name_stub):
        name = pd.read_csv(os.path.join(self.src, 'p%s.csv' % name_stub))
        name['p%s' % name_stub] = name['p%s' % name_stub].str.upper()
        name = name[['npi', 'p%s' % name_stub]].drop_duplicates()
        name = (name.merge(self.entity.query('entity==1'))
                    .drop(columns=['entity']))
        name = purge_nulls(name, 'p%s' % name_stub, ['npi'])
        return name

    def get_fname(self):
        if hasattr(self, 'fname'):
            return
        self.fname = self.get_name('fname')

    def get_mname(self):
        if hasattr(self, 'mname'):
            return
        self.mname = self.get_name('mname')

    def get_lname(self):
        if hasattr(self, 'lname'):
            return
        self.lname = self.get_name('lname')

    def get_nameoth(self, name_stub):
        nameoth = pd.read_csv(os.path.join(self.src, 'p%s.csv' % name_stub))
        nameoth = nameoth.dropna()
        nameoth['p%s' % name_stub] = nameoth['p%s' % name_stub].str.upper()
        nameoth = nameoth[['npi', 'p%s' % name_stub]].drop_duplicates()
        return nameoth

    def get_fnameoth(self):
        if hasattr(self, 'fnameoth'):
            return
        self.fnameoth = self.get_nameoth('fnameoth')

    def get_mnameoth(self):
        if hasattr(self, 'mnameoth'):
            return
        self.mnameoth = self.get_nameoth('mnameoth')

    def get_lnameoth(self):
        if hasattr(self, 'lnameoth'):
            return
        self.lnameoth = self.get_nameoth('lnameoth')

    def get_locstatename(self):
        if hasattr(self, 'locstatename'):
            return
        src = self.src
        locstatename = pd.read_csv(os.path.join(src, 'plocstatename.csv'))
        self.locstatename = locstatename

    def get_cred(self, name_stub):
        src = self.src
        name_stub = 'p%s' % name_stub
        credential = pd.read_csv(os.path.join(src, '%s.csv' % name_stub))
        credential = credential.dropna()
        credential[name_stub] = credential[name_stub].str.upper()
        credential = credential[['npi', name_stub]].drop_duplicates()
        credential = (credential.merge(self.entity.query('entity==1'))
                                .drop(columns=['entity']))
        credential[name_stub] = credential[name_stub].str.replace('.', '')
        credential = (credential.reset_index()
                                .merge(
                                    (credential[name_stub].str
                                                          .split(',',
                                                                 expand=True)
                                                          .stack()
                                                          .reset_index()),
                                    left_on='index', right_on='level_0')
                                .drop(columns=['index', name_stub,
                                               'level_0', 'level_1'])
                                .rename(columns={0: name_stub}))
        credential[name_stub] = credential[name_stub].str.strip()
        credential = credential.drop_duplicates()
        return credential

    def get_credential(self):
        if hasattr(self, 'credential'):
            return
        self.credential = self.get_nameoth('credential')

    def get_credentialoth(self):
        if hasattr(self, 'credentialoth'):
            return
        self.credentialoth = self.get_nameoth('credentialoth')

    def get_credentials(self):
        if hasattr(self, 'credentials'):
            return
        self.get_credential()
        self.get_credentialoth()
        credential = self.credential
        credentialoth = self.credentialoth
        self.credentials = credential.append(
            credentialoth.rename(
                columns={'pcredentialoth': 'pcredential'})).drop_duplicates()

    def get_taxcode(self):
        taxcode = pd.read_csv(os.path.join(src, 'ptaxcode.csv'))
        taxcode = taxcode[['npi', 'ptaxcode']].drop_duplicates()
        taxonomy_path = ('/home/akilby/Packages/claims_data/src/claims_data/'
                         'data/Provider Taxonomies - Labeled.csv')
        tax = pd.read_csv(taxonomy_path)
        tax.columns = ['EntityType', 'Type', 'Classification',
                       'Specialization', 'TaxonomyCode']
        pa = (tax.query('Classification == "Physician Assistant"')
                 .TaxonomyCode
                 .tolist())
        np = (tax.query('Classification == "Nurse Practitioner"')
                 .TaxonomyCode
                 .tolist())
        mddo = (tax.query('Type=="Allopathic & Osteopathic Physicians"')
                   .TaxonomyCode
                   .tolist())
        student = tax.query('TaxonomyCode=="390200000X"').TaxonomyCode.tolist()
        taxcode.loc[taxcode.ptaxcode.isin(pa), 'cat'] = 'pa'
        taxcode.loc[taxcode.ptaxcode.isin(np), 'cat'] = 'np'
        taxcode.loc[taxcode.ptaxcode.isin(mddo), 'cat'] = 'mddo'
        taxcode.loc[taxcode.ptaxcode.isin(student), 'cat'] = 'student'
        taxcode = (taxcode.merge(self.entity.query('entity==1'))
                          .drop(columns=['entity']))
        self.taxcode = taxcode

    def get_fullnames(self):
        if hasattr(self, 'fullnames'):
            return
        self.get_fname()
        self.get_mname()
        self.get_lname()
        self.get_fnameoth()
        self.get_mnameoth()
        self.get_lnameoth()

        name_list = ['pfname', 'pmname', 'plname']
        oth_list = ['pfnameoth', 'pmnameoth', 'plnameoth']
        ren = {'plnameoth': 'plname',
               'pfnameoth': 'pfname',
               'pmnameoth': 'pmname'}

        fullnames = pd.merge(self.fname, self.lname, how='outer')
        fullnames = pd.merge(fullnames, self.mname, how='outer')
        fullnames = fullnames[['npi'] + name_list]
        merged = (self.fnameoth.merge(self.lnameoth, how='outer')
                               .merge(self.mnameoth, how='outer')
                               .merge(self.fname, how='left')
                               .merge(self.lname, how='left')
                               .merge(self.mname, how='left'))
        merged.loc[merged.pfnameoth.isnull(), 'pfnameoth'] = merged.pfname
        merged.loc[merged.plnameoth.isnull(), 'plnameoth'] = merged.plname

        merged2 = merged.copy()
        merged.loc[merged.pmnameoth.isnull(), 'pmnameoth'] = merged.pmname

        def remove_duplicated_names(merged):
            merged = (merged.drop(columns=name_list)
                            .drop_duplicates())
            merged = (merged.merge(fullnames,
                                   left_on=['npi'] + oth_list,
                                   right_on=['npi'] + name_list,
                                   how='left', indicator=True)
                            .query('_merge=="left_only"')
                            .drop(columns=name_list + ['_merge']))
            return merged

        merged = remove_duplicated_names(merged)
        merged2 = remove_duplicated_names(merged2)

        merged = merged.append(merged2).drop_duplicates()
        merged['othflag'] = 1
        fullnames['othflag'] = 0
        fullnames = (fullnames.append(merged.rename(columns=ren))
                              .sort_values(['npi', 'othflag'])
                              .reset_index(drop=True))
        self.fullnames = self.fullnames_clean(fullnames)

    def fullnames_clean(self, df):
        for symbol in ['?', '+', '[', ']', ';']:
            df = purge_symbol(df, 'pfname', symbol, ['npi'])
            df = purge_symbol(df, 'pmname', symbol, ['npi'])
            df = purge_symbol(df, 'plname', symbol, ['npi'])
        df['pmname'] = df.pmname.apply(lambda x: _middle_initials(x))
        df['pfname'] = df.pfname.str.strip()
        df['pmname'] = df.pmname.str.strip()
        df['plname'] = df.plname.str.strip()
        for symbol in ['+', '[', ']', ';']:
            df['pfname'] = df.pfname.apply(lambda x: _delete(x, symbol))
            df['pmname'] = df.pmname.apply(lambda x: _delete(x, symbol))
            df['plname'] = df.plname.apply(lambda x: _delete(x, symbol))
        df = (df.fillna('')
                .groupby(['npi', 'pfname', 'pmname', 'plname'])
                .min()
                .reset_index())
        df = self.parens_clean(df)
        df = (df.fillna('')
                .groupby(['npi', 'pfname', 'pmname', 'plname'])
                .min()
                .reset_index())
        return df

    def parens_clean(self, df):
        parens = df[df.plname.apply(lambda x: _in(x, '('))]
        noparens = df[~df.plname.apply(lambda x: _in(x, '('))]
        parens = parens.reset_index(drop=True)
        parens['plname0'] = parens.plname.str.split('(').str[0].str.strip()
        parens['plname1'] = parens.plname.str.split('(').str[1].str.strip()
        parens['plname2'] = parens.plname1.str.split(')').str[0].str.strip()
        parens['plname3'] = parens.plname1.str.split(')').str[1].str.strip()
        parens.drop(columns=['plname1'], inplace=True)
        parens['plname2'] = (parens.plname2
                                   .str.replace('FORMERLY', '')
                                   .str.replace('MAIDEN NAME', '')
                                   .str.replace("DIDN'T HAVE LAST NAME", "")
                                   .str.replace('-MAIDEN', '')
                                   .str.replace('MAIDEN', '')
                                   .str.replace('CHANGE FROM', '')
                                   .str.replace('BIRTH', '')
                                   .str.replace('NEW NAME', '')
                                   .str.replace('MARRIED NAME', '')
                                   .str.replace('MARRIED', '')
                                   .str.replace('SOLE PROPRIETOR', '')
                                   .str.replace('PREVIOUSLY', '')
                                   .str.replace('CURRENT NAME', '')
                                   .str.replace('PREVIOUS NAME', '')
                                   .str.replace('AND ALSO, ', '')
                                   .str.replace('CHANGED FROM', '')
                                   .str.replace('ALSO', '')
                                   .str.replace(' - USED BOTH', ''))
        parens['plname3'] = (parens.plname3
                                   .str.replace(',1ST MARRIED-', '')
                                   .str.replace('-FORMER MARRIAGE', ''))
        parens.loc[(parens.plname2 == 'OR'), 'plname2'] = ''
        parens.loc[(parens.plname2 == ' OR'), 'plname2'] = ''
        parens.loc[(parens.plname ==
                    '(1) ALLEN, (2) BRAACK'), 'plname2'] = 'ALLEN'
        parens.loc[(parens.plname ==
                    '(1) ALLEN, (2) BRAACK'), 'plname3'] = 'BRAACK'
        parens = (parens.set_index(['npi', 'pfname',
                                    'pmname', 'plname', 'othflag'])
                        .stack(0)
                        .reset_index()
                        .drop(columns='level_5')
                        .rename(columns={0: 'plnamealt'}))
        parens['plnamealt'] = parens.plnamealt.str.strip()
        parens = parens.query('plnamealt!=""')
        parens = parens.reset_index(drop=True)
        parens.loc[(parens.plname == 'JAMES AND (CHARMOY, LCSW, LCADC)'),
                   'plnamealt'] = 'CHARMOY'
        parens['plnamealt'] = parens.plnamealt.str.replace(', ', '')
        parens = (parens.drop(columns='plname')
                        .rename(columns={'plnamealt': 'plname'})
                        .drop_duplicates())
        df = noparens.append(parens)
        df.loc[(df.plname == '1)MORENO 2) MORENO'), 'plname'] = 'MORENO'
        df.loc[(df.pfname == '1)PATRICIA 2)BEATA'), 'pfname'] = 'BEATA'
        df.loc[(df.pmname == '1) ANN'), 'pmname'] = 'ANN'
        df.loc[(df.plname == 'P)ERZEL'), 'plname'] = 'PERZEL'
        df = df[~df.plname.apply(lambda x: _in(x, ')'))]
        parens = df[df.pfname.apply(lambda x: _in(x, '('))]
        noparens = df[~df.pfname.apply(lambda x: _in(x, '('))]
        parens = parens.reset_index(drop=True)
        parens['pfname0'] = parens.pfname.str.split('(').str[0].str.strip()
        parens['pfname1'] = parens.pfname.str.split('(').str[1].str.strip()
        parens['pfname2'] = parens.pfname1.str.split(')').str[0].str.strip()
        parens['pfname3'] = parens.pfname1.str.split(')').str[1].str.strip()
        parens.drop(columns=['pfname1'], inplace=True)
        parens['pfname2'] = (parens.pfname2
                                   .str.replace('LEGAL NAME', '')
                                   .str.replace('408', '')
                                   .str.replace('510', ''))
        parens = (parens.set_index(['npi', 'pfname',
                                    'pmname', 'plname', 'othflag'])
                        .stack(0)
                        .reset_index()
                        .drop(columns='level_5')
                        .rename(columns={0: 'pfnamealt'}))
        parens['pfnamealt'] = parens.pfnamealt.str.strip()
        parens = parens.query('pfnamealt!=""')
        parens = parens.reset_index(drop=True)
        parens = (parens.drop(columns='pfname')
                        .rename(columns={'pfnamealt': 'pfname'})
                        .drop_duplicates())
        df = noparens.append(parens)
        df = (df.query('pfname!="NONE" or plname!="NONE"')
                .reset_index(drop=True).fillna(''))
        df['pfname'] = df.pfname.apply(lambda x: _delete(x, ')'))
        return df


def purge_nulls(df, var, mergeon):
    '''
    For rows that are null, drop from data only if they are present elsewhere
    '''
    missings = df[df[var].isnull()].merge(df[~df[var].isnull()], on=mergeon)
    missings = missings[['npi', '%s_x' % var]].rename(
        columns={'%s_x' % var: var})
    missings = missings.drop_duplicates()
    df = (df.merge(missings, indicator=True, how='left')
            .query('_merge=="left_only"')
            .drop(columns=['_merge']))
    return df


def purge_symbol(df, var, symbol, mergeon):
    bad = df[df[var].apply(lambda x: _in(x, symbol))]
    good = df[~df[var].apply(lambda x: _in(x, symbol))]
    bad = bad.merge(good, on=mergeon)
    xvars = ['pfname_x', 'pmname_x', 'plname_x']
    bad = bad[['npi'] + xvars].drop_duplicates()
    df = df.merge(bad,
                  left_on=['npi', 'pfname', 'pmname', 'plname'],
                  right_on=['npi'] + xvars,
                  indicator=True, how='left')
    return df.query('_merge!="both"').drop(columns=xvars + ['_merge'])


def _middle_initials(x):
    '''Only deletes periods in middle initials of the format A.'''
    if not pd.isnull(x):
        if re.match('^([A-Za-z]?)\.$', x):
            return x.replace('.', '')
    return x


def _in(x, obj): return ((True if obj in x else False) if not pd.isnull(x)
                         else False)


def _delete(x, obj): return ((x.replace(obj, '') if obj in x else x)
                             if not pd.isnull(x)
                             else x)


def _in_multi(x, list_objs): return any([_in(x, obj) for obj in list_objs])



# samhsa = pd.read_csv(
#     '/work/akilby/npi/FOIA_12312019_datefilled_clean_NPITelefill.csv',
#     low_memory=False)
# samhsa = samhsa.drop(columns=['Unnamed: 0', 'LocatorWebsiteConsent',
#                               'First name', 'Last name', 'npi', 'NPI state',
#                               'Date', 'CurrentPatientLimit', 'Telephone',
#                               'zip', 'zcta5', 'geoid', 'Index',
#                               'NumberPatientsCertifiedFor', 'statecode'])
# samhsa['NameFull'] = samhsa.NameFull.str.upper()
# 
# uvars = ['NameFull', 'PractitionerType', 'DateLastCertified', 'Street1',
#          'City', 'State', 'Zip', 'County', 'Phone']
# 
# mindate = (samhsa[uvars + ['DateGranted']].dropna()
#                                           .groupby(uvars).min().reset_index())
# 
# samhsa = (samhsa.fillna('')
#                 .groupby(['NameFull', 'PractitionerType',
#                           'DateLastCertified', 'Street1', 'Street2',
#                           'City', 'State', 'Zip', 'County', 'Phone'])
#                 .agg({'WaiverType': max})
#                 .reset_index())
# 
# samhsa = samhsa.merge(mindate, how='left')
# 
# badl = ['(JR.)', '(M.D.)', '(RET.)', 'M., PH', 'M. D.', 'M.D.,',
#         'M.D., PHD, DABA,',
#         'M .D.', 'M.D.', 'MD', 'NP', 'D.O.', 'PA', 'DO', '.D.', 'MPH',
#         'PH.D.', 'JR', 'M.D', 'PHD', 'P.A.', 'FNP', 'PA-C', 'M.P.H.',
#         'N.P.', 'III', 'SR.', 'D.', '.D', 'FASAM', 'JR.', 'MS',
#         'D', 'FAAFP', 'SR', 'D.O', 'CNS', 'F.A.S.A.M.', 'FNP-BC', 'P.C.',
#         'MBA', 'M.S.', 'PH.D', 'FACP', 'M.P.H', 'CNM',
#         'NP-C', 'MR.', 'MDIV', 'FACEP', 'PLLC', 'M.A.', 'LLC', 'MR',
#         'DNP', 'PHD.', 'FNP-C', 'MD.', 'CNP', 'J.D.', 'IV', 'F.A.P.A.',
#         'DR.', 'M.D,', 'DABPM', 'M,D.', 'MS.', 'FACOOG']
# 
# badl2 = (pd.read_csv('/work/akilby/npi/stubs_rev.csv')
#            .rename(columns={'Unnamed: 2': 'flag'})
#            .query('flag==1')['Unnamed: 0']
#            .tolist())
# 
# badl = badl + badl2
# 
# 
# def remove_suffixes(samhsa, badl):
#     for b in badl:
#         samhsa.loc[samhsa.NameFull.apply(lambda x: x.endswith(' ' + b)),
#                    'Credential String'] = (samhsa.NameFull
#                                                  .apply(lambda x:
#                                                         x[len(x)-len(b):]))
#         samhsa['NameFull'] = (samhsa.NameFull
#                                     .apply(lambda x: x[:len(x)-len(b)]
#                                            if x.endswith(' ' + b) else x))
#         samhsa['NameFull'] = samhsa['NameFull'].str.strip()
#         samhsa['NameFull'] = (samhsa.NameFull
#                                     .apply(lambda x: x[:-1]
#                                            if x.endswith(',') else x))
#     return samhsa
# 
# 
# samhsa = remove_suffixes(samhsa, badl)
# samhsa['NameFull'] = samhsa.NameFull.apply(
#     lambda x: re.sub('(.*\s[A-Z]?)(\.)(\s)', r'\1\3', x))
# 
# 
# samhsa = remove_suffixes(samhsa, badl)
# assert remove_suffixes(samhsa.copy(), badl).equals(samhsa.copy())
# 
# 
# df.loc[df.pmname=='', 'name']=df.pfname + ' ' + df.plname 
# df.loc[df.pmname!='', 'name']=df.pfname + ' ' + df.pmname + ' ' + df.plname 
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
