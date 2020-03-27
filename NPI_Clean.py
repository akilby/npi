import os
import re

import pandas as pd

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
        taxcode.loc[taxcode.ptaxcode.isin(pa), 'cat'] = 'PA'
        taxcode.loc[taxcode.ptaxcode.isin(np), 'cat'] = 'NP'
        taxcode.loc[taxcode.ptaxcode.isin(mddo), 'cat'] = 'MD/DO'
        taxcode.loc[taxcode.ptaxcode.isin(student), 'cat'] = 'MD/DO Student'
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

        def get_expanded_fullnames(self):
            if hasattr(self, 'expanded_fullnames'):
                return
            self.get_fullnames()
            f = self.fullnames.copy()
            f.drop(columns=['othflag', 'name'], inplace=True)
            idvar = 'npi'
            (firstname, middlename, lastname) = ('pfname', 'pmname', 'plname')
            self.expanded_fullnames = expand_names_in_sensible_ways(
                f, idvar, firstname, middlename, lastname)


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


def expand_names_in_sensible_ways(df, idvar, firstname, middlename, lastname):
    # one middle initial
    df['minit'] = df[middlename].str[:1]
    # no middle name
    df['mblank'] = ''
    expanded_full = (df.drop(columns=['minit', 'mblank'])
                       .append((df.drop(columns=[middlename, 'mblank'])
                                  .rename(columns={'minit': middlename})))
                       .append((df.drop(columns=[middlename, 'minit'])
                                  .rename(columns={'mblank': middlename})))
                       .drop_duplicates())
    expanded_full2 = expanded_full.copy()
    # delete all periods
    for nam in [firstname, middlename, lastname]:
        expanded_full2[nam] = expanded_full[nam].str.replace('.', '')
    expanded_full = (expanded_full.append(expanded_full2)
                                  .drop_duplicates()
                                  .sort_values(idvar)
                                  .reset_index(drop=True))
    # turn into one name column
    expanded_full.loc[expanded_full[middlename] == '', 'name'] = (
        expanded_full[firstname] + ' ' + expanded_full[lastname])
    expanded_full.loc[expanded_full[middlename] != '', 'name'] = (
        expanded_full[firstname] + ' ' + expanded_full[middlename]
        + ' ' + expanded_full[lastname])
    k = [idvar, firstname, middlename, lastname, 'name']
    expanded_full = expanded_full[k].drop_duplicates()
    return expanded_full