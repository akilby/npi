import os
import re
import time

import pandas as pd

unmatched = pd.read_csv('/work/akilby/npi/raw_samhsa/check.csv')
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
                               .merge(self.mnmeoth, how='outer')
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
        self.fullnames = fullnames


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




# nonames = df[~df.npi.isin(df.dropna().npi)]

# df = df.dropna()
# df = df.reset_index(drop=True)

# states = locstatename[~locstatename.npi.isin(nonames.npi)]
# states = states.dropna()
# states = states.reset_index(drop=True)


def _in(x, obj): return True if obj in x else False


# df = df[~df.plname.apply(lambda x: _in(x, '?'))].reset_index(drop=True)
# df = df[~df.pfname.apply(lambda x: _in(x, '?'))].reset_index(drop=True)
# df['plname'] = df['plname'].str.replace('+', '')
# df['plname'] = df['plname'].str.replace('[', '')
# df['plname'] = df['plname'].str.replace(';', '')

# df['pfname'] = df['pfname'].str.replace('+', '')
# df['pfname'] = df['pfname'].str.replace(';', '')

# df_parens = df[df.plname.apply(lambda x: _in(x, '('))]
# df = df[~df.plname.apply(lambda x: _in(x, '('))]

# df_parens['plname_alt'] = df_parens.plname.str.split('(').str[1].str.split(')').str[0]
# df_parens['plname_alt2'] = df_parens.apply(lambda x: x.plname.replace("(" + x.plname_alt + ")", '').strip(), axis=1)
# df_parens['plname_alt'] = df_parens.plname_alt.str.strip()
# df_parens = df_parens.set_index(['npi','pfname']).stack().reset_index().drop(columns=['level_2']).rename(columns={0:'plname'})

# df = df.append(df_parens).reset_index(drop=True)

# df_samhsa = unmatched[['NameFull', 'State', 'DateLastCertified']]
# df_samhsa = df_samhsa.drop_duplicates().reset_index(drop=True)
# df_samhsa['NameFull'] = df_samhsa.NameFull.str.upper()
# df_samhsa = df_samhsa.drop_duplicates().reset_index(drop=True)


# badl = ['(JR.)', '(M.D.)', '(RET.)', 'M., PH', 'M.D.,', 'M.D., PHD, DABA,',
#         'M .D.', 'M.D.', 'MD', 'NP', 'D.O.', 'PA', 'DO', '', '.D.', 'MPH',
#         'PH.D.', 'JR', 'M.D', 'PHD', 'P.A.', 'FNP', 'PA-C', 'M.P.H.',
#         'N.P.', 'III', 'SR.', 'D.', '.D', 'FASAM', 'JR.', 'MS',
#         'D', 'FAAFP', 'SR', 'D.O', 'CNS', 'F.A.S.A.M.', 'FNP-BC', 'P.C.',
#         'MBA', 'M.S.', 'PH.D', 'FACP', 'M.P.H', 'DANIEL', 'KHAN', 'CNM',
#         'NP-C', 'MR.', 'MDIV', 'FACEP', 'PLLC', 'M.A.', 'LLC', 'MR',
#         'DNP', 'PHD.', 'FNP-C', 'MD.', 'CNP', 'J.D.', 'IV', 'F.A.P.A.',
#         'DR.', 'M.D,', 'DABPM', 'M,D.', 'MS.']


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
