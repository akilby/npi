"""
Notes: for names, and fullnames, only for entity=1
Also, stored at the individual, not individual-month level

For addresses, should really add the other practice addresses (ploc2) that got
added to the NPI in recent years in different files
"""

import os
import re

import pandas as pd
from utility_data.taxonomies import provider_taxonomies

from .download.medical_schools import final_data_path as med_school_path
from .utils.utils import longprint

src = '/work/akilby/npi/data/'


class NPI(object):
    def __init__(self, src=src, npis=None, entities=[1, 2]):
        self.src = src
        self.npis = npis
        self.entities = _normalize_entities(entities)
        self.get_entity()

    def retrieve(self, thing):
        getattr(self, f'get_{thing}')()

    def retrieve_all(self):
        fc = [x.replace('get_', '') for x in dir(self) if x.startswith('get_')]
        for f in fc:
            self.retrieve(f)

    def get_entity(self):
        if hasattr(self, 'entity'):
            return
        from .utils.globalcache import c
        self.entity = c.get_entity(self.src, self.npis)

    def get_npideactdate(self):
        if hasattr(self, 'npideactdate'):
            return
        from .utils.globalcache import c
        self.npideactdate = c.get_deactdate(self.src, self.npis)

    def get_npireactdate(self):
        if hasattr(self, 'npireactdate'):
            return
        from .utils.globalcache import c
        self.npireactdate = c.get_reactdate(self.src, self.npis)

    def get_removaldate(self):
        if hasattr(self, 'removaldate'):
            return
        self.get_npideactdate()
        self.get_npireactdate()
        removaldate = (self.npideactdate
                           .merge(self.npideactdate
                                      .merge(self.npireactdate)
                                      .query('npireactdate>=npideactdate'),
                                  how='left', indicator=True)
                           .query('_merge!="both"')
                           .drop(columns=['npireactdate', '_merge']))
        if self.entities == 1 or self.entities == [1]:
            removaldate = (removaldate.merge(self.entity.query('entity==1'))
                                      .drop(columns=['entity']))
        elif self.entities == 2 or self.entities == [2]:
            removaldate = (removaldate.merge(self.entity.query('entity==2'))
                                      .drop(columns=['entity']))
        self.removaldate = removaldate

    def get_pfname(self):
        if hasattr(self, 'pfname') or self.entities in [2, [2]]:
            return
        from .utils.globalcache import c
        self.pfname = c.get_name(self.src, self.npis, self.entity, 'pfname')

    def get_pmname(self):
        if hasattr(self, 'pmname') or self.entities in [2, [2]]:
            return
        from .utils.globalcache import c
        self.pmname = c.get_name(self.src, self.npis, self.entity, 'pmname')

    def get_plname(self):
        if hasattr(self, 'plname') or self.entities in [2, [2]]:
            return
        from .utils.globalcache import c
        self.plname = c.get_name(self.src, self.npis, self.entity, 'plname')

    def get_pfnameoth(self):
        if hasattr(self, 'pfnameoth') or self.entities in [2, [2]]:
            return
        from .utils.globalcache import c
        self.pfnameoth = c.get_nameoth(
            self.src, self.npis, self.entity, 'pfnameoth')

    def get_pmnameoth(self):
        if hasattr(self, 'pmnameoth') or self.entities in [2, [2]]:
            return
        from .utils.globalcache import c
        self.pmnameoth = c.get_nameoth(
            self.src, self.npis, self.entity, 'pmnameoth')

    def get_plnameoth(self):
        if hasattr(self, 'plnameoth') or self.entities in [2, [2]]:
            return
        from .utils.globalcache import c
        self.plnameoth = c.get_nameoth(
            self.src, self.npis, self.entity, 'plnameoth')

    def get_pcredential(self):
        if hasattr(self, 'pcredential') or self.entities in [2, [2]]:
            return
        from .utils.globalcache import c
        self.pcredential = c.get_cred(
            self.src, self.npis, self.entity, 'pcredential')

    def get_pcredentialoth(self):
        if hasattr(self, 'pcredentialoth') or self.entities in [2, [2]]:
            return
        from .utils.globalcache import c
        self.pcredentialoth = c.get_cred(
            self.src, self.npis, self.entity, 'pcredentialoth')

    def get_plocline1(self):
        if hasattr(self, 'plocline1'):
            return
        self.get_removaldate()
        from .utils.globalcache import c
        self.plocline1 = c.get_address(self.src,
                                       self.npis,
                                       self.entity,
                                       self.removaldate,
                                       self.entities,
                                       'plocline1')

    def get_plocline2(self):
        if hasattr(self, 'plocline2'):
            return
        self.get_removaldate()
        from .utils.globalcache import c
        self.plocline2 = c.get_address(self.src,
                                       self.npis,
                                       self.entity,
                                       self.removaldate,
                                       self.entities,
                                       'plocline2')

    def get_ploccityname(self):
        if hasattr(self, 'ploccityname'):
            return
        self.get_removaldate()
        from .utils.globalcache import c
        self.ploccityname = c.get_address(self.src,
                                          self.npis,
                                          self.entity,
                                          self.removaldate,
                                          self.entities,
                                          'ploccityname')

    def get_plocstatename(self):
        if hasattr(self, 'plocstatename'):
            return
        self.get_removaldate()
        from .utils.globalcache import c
        self.plocstatename = c.get_address(self.src,
                                           self.npis,
                                           self.entity,
                                           self.removaldate,
                                           self.entities,
                                           'plocstatename')

    def get_ploczip(self):
        if hasattr(self, 'ploczip'):
            return
        self.get_removaldate()
        from .utils.globalcache import c
        self.ploczip = c.get_address(self.src,
                                     self.npis,
                                     self.entity,
                                     self.removaldate,
                                     self.entities,
                                     'ploczip')

    def get_ploctel(self):
        # Is .upper()ing a problem?
        if hasattr(self, 'loctel'):
            return
        self.get_removaldate()
        from .utils.globalcache import c
        loctel = c.get_address(self.src,
                               self.npis,
                               self.entity,
                               self.removaldate,
                               self.entities,
                               'ploczip')
        self.ploctel = loctel

    def get_credentials(self):
        if hasattr(self, 'credentials') or self.entities in [2, [2]]:
            return
        self.get_pcredential()
        self.get_pcredentialoth()
        credential = self.pcredential
        credentialoth = self.pcredentialoth
        credentials = credential.append(
            credentialoth.rename(
                columns={'pcredentialoth': 'pcredential'})).drop_duplicates()
        credentials['pcredential_stripped'] = (credentials.pcredential
                                                          .str.replace('.', '')
                                                          .str.replace(' ', '')
                                                          .str.strip()
                                                          .str.upper())
        self.credentials = credentials

    def get_fullnames(self):
        if hasattr(self, 'fullnames') or self.entities in [2, [2]]:
            return
        self.get_pfname()
        self.get_pmname()
        self.get_plname()
        self.get_pfnameoth()
        self.get_pmnameoth()
        self.get_plnameoth()

        from .utils.globalcache import c
        self.fullnames = c.get_fullnames(self.pfname,
                                         self.pmname,
                                         self.plname,
                                         self.pfnameoth,
                                         self.pmnameoth,
                                         self.plnameoth)

    def get_ptaxcode(self):
        if hasattr(self, 'ptaxcode'):
            return
        from .utils.globalcache import c
        taxcode = c.get_taxcode(
            self.src, self.npis, self.entity, self.entities)
        self.ptaxcode = taxcode

    def get_expanded_fullnames(self):
        if hasattr(self, 'expanded_fullnames') or self.entities in [2, [2]]:
            return
        self.get_fullnames()
        f = self.fullnames.copy()
        f.drop(columns=['othflag'], inplace=True)
        idvar = 'npi'
        (firstname, middlename, lastname) = ('pfname', 'pmname', 'plname')
        self.expanded_fullnames = expand_names_in_sensible_ways(
            f, idvar, firstname, middlename, lastname)

    def get_training_dates(self):
        if hasattr(self, 'training_dates'):
            return
        from .utils.globalcache import c
        training_details = c.get_training_dates(self.src, self.entity)
        self.training_dates = training_details


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


def _normalize_entities(entities):
    if entities == [1] or entities == 1:
        return [1]
    elif entities == [2] or entities == 2:
        return [2]
    elif entities == [1, 2] or entities == [2, 1]:
        return [1, 2]
    else:
        raise ValueError("Value %s not a valid value for entities" % entities)


def expand_names_in_sensible_ways(df, idvar, firstname, middlename, lastname):
    '''
    For custom fuzzy matching
    '''
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


def _read_csv_npi(readfile, npis):
    chunks = pd.read_csv(readfile, chunksize=500000)
    li = []
    for chunk in chunks:
        li.append(chunk[chunk['npi'].isin(npis)])
    return pd.concat(li)


def read_csv_npi(rfile, npis):
    return (pd.read_csv(rfile) if not isinstance(npis, pd.Series)
            else _read_csv_npi(rfile, npis))


def get_entity(src, npis):
    """
    Returns NPI entity type across all NPIs (invariant over time)
    """
    entity = read_csv_npi(os.path.join(src, 'entity.csv'), npis)
    entity = entity.dropna()
    entity['entity'] = entity.entity.astype("int")
    entity = entity[['npi', 'entity']].drop_duplicates()
    return entity


def get_deactdate(src, npis):
    deact = read_csv_npi(os.path.join(src, 'npideactdate.csv'), npis)
    deact = deact.drop(columns='month').dropna().drop_duplicates()
    deact['npideactdate'] = pd.to_datetime(deact['npideactdate'])
    return deact.groupby('npi', as_index=False).max()


def get_reactdate(src, npis):
    react = read_csv_npi(os.path.join(src, 'npireactdate.csv'), npis)
    react = react.drop(columns='month').dropna().drop_duplicates()
    react['npireactdate'] = pd.to_datetime(react['npireactdate'])
    return react.groupby('npi', as_index=False).max()


def get_name(src, npis, entity, name_stub):
    """
    Retrieves pfname, pmname, and plname
    Only for entity type 1
    Returns non-temporal data: all names associated with a given NPI
    """
    name = read_csv_npi(os.path.join(src, '%s.csv' % name_stub), npis)
    name['%s' % name_stub] = name[name_stub].str.upper()
    name = name[['npi', name_stub]].drop_duplicates()
    assert name.dropna().merge(entity).entity.value_counts().index == [1]
    name = (name.merge(entity.query('entity==1')).drop(columns=['entity']))
    name = purge_nulls(name, '%s' % name_stub, ['npi'])
    return name


def get_nameoth(src, npis, entity, name_stub):
    """
    Retrieves pfnameoth, pmnameoth, and plnameoth
    Only for entity type 1
    Returns non-temporal data: all names associated with a given NPI
    """
    nameoth = read_csv_npi(os.path.join(src, '%s.csv' % name_stub), npis)
    assert nameoth.dropna().merge(entity).entity.value_counts().index == [1]
    nameoth = nameoth.dropna()
    nameoth = (nameoth.merge(entity.query('entity==1'))
                      .drop(columns=['entity']))
    nameoth[name_stub] = nameoth[name_stub].str.upper()
    nameoth = nameoth[['npi', name_stub]].drop_duplicates()
    return nameoth


def categorize_taxcodes(df):
    assert set(['entity', 'ptaxcode']).issubset(set(df.columns))
    tax = provider_taxonomies()
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
    df.loc[(df.ptaxcode.isin(pa) & df.entity == 1), 'cat'] = 'PA'
    df.loc[(df.ptaxcode.isin(np) & df.entity == 1), 'cat'] = 'NP'
    df.loc[(df.ptaxcode.isin(mddo) & df.entity == 1), 'cat'] = 'MD/DO'
    df.loc[(df.ptaxcode.isin(student)
            & df.entity == 1), 'cat'] = 'MD/DO Student'
    return df


def get_taxcode(src, npis, entity, entities, temporal=False):
    """
    Retrieves taxonomy codes (including all 15 entries if necessary)
    Entity type 1 or 2 can have a taxcode
    Returns non-temporal data unless otherwise specified;
    all taxcodes associated with a given NPI
    Assigns a category only to entity types 1
    Removes erroneous entries with the MD student code that
    later do not become doctors; this procedure is not possible for
    young trainees
    """
    taxcode = read_csv_npi(os.path.join(src, 'ptaxcode.csv'), npis)
    if temporal:
        taxcode_all = taxcode.copy()
        taxcode_all['month'] = pd.to_datetime(taxcode_all.month)
        taxcode_all = taxcode_all.merge(entity)
    if not (temporal and entity in [2, [2]]):
        taxcode = taxcode[['npi', 'ptaxcode']].drop_duplicates()
    if entities in [1, [1]] or entities == [1, 2] or entities == [2, 1]:
        taxcode_e1 = (taxcode.merge(entity.query('entity==1')))
        taxcode_e1 = categorize_taxcodes(taxcode_e1)
        training_future = (pd.concat([taxcode_e1,
                                      pd.get_dummies(taxcode_e1.cat,
                                                     dummy_na=True)
                                      ],
                                     axis=1)
                             .drop(columns=['cat', 'ptaxcode', 'entity'])
                             .groupby('npi').max())
        training_future.columns = ['MDDO', 'MDDOStudent', 'NP', 'PA', 'NaN']
        not_docs = training_future.query(
            'MDDOStudent==1 and (NaN+PA+NP)>0 and MDDO==0').reset_index()
        not_docs['cat'] = "MD/DO Student"
        taxcode_e1 = (taxcode_e1.merge(not_docs[['npi', 'cat']],
                                       how='left', indicator=True)
                                .query('_merge=="left_only"')
                                .drop(columns='_merge'))
    if entities in [2, [2]] or entities == [1, 2] or entities == [2, 1]:
        taxcode_e2 = (taxcode.merge(entity.query('entity==2')))

    if not temporal:
        if entities in [1, [1]]:
            return taxcode_e1
        elif entities in [2, [2]]:
            return taxcode_e2
        elif entities == [1, 2] or entities == [2, 1]:
            return taxcode_e1.append(taxcode_e2)
    else:
        if entities in [1, [1]] or entities == [1, 2] or entities == [2, 1]:
            if entities in [1, [1]]:
                taxcode_all = taxcode_all.query('entity==1')
            taxcode_all = categorize_taxcodes(taxcode_all)
            taxcode_all = (taxcode_all.merge(not_docs[['npi', 'cat']],
                                             how='left', indicator=True)
                                      .query('_merge=="left_only"')
                                      .drop(columns='_merge'))
            return taxcode_all
        elif entities in [2, [2]]:
            return taxcode_all.query('entity==2')


def get_cred(src, npis, entity, name_stub):
    """
    Retrieves credential, credentialoth
    Only for entity type 1
    Returns non-temporal data: all credentials associated with a given NPI
    """
    credential = read_csv_npi(os.path.join(src, '%s.csv' % name_stub), npis)
    assert credential.dropna().merge(entity).entity.value_counts().index == [1]
    credential = credential.dropna()
    credential = (credential.merge(entity.query('entity==1'))
                            .drop(columns=['entity']))
    credential[name_stub] = credential[name_stub].str.upper()
    credential = credential[['npi', name_stub]].drop_duplicates()
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


def get_fullnames(pfname, pmname, plname, pfnameoth, pmnameoth, plnameoth):
    name_list = ['pfname', 'pmname', 'plname']
    oth_list = ['pfnameoth', 'pmnameoth', 'plnameoth']
    ren = {'plnameoth': 'plname',
           'pfnameoth': 'pfname',
           'pmnameoth': 'pmname'}

    fullnames = pd.merge(pfname, plname, how='outer')
    fullnames = pd.merge(fullnames, pmname, how='outer')
    fullnames = fullnames[['npi'] + name_list]
    merged = (pfnameoth.merge(plnameoth, how='outer')
                       .merge(pmnameoth, how='outer')
                       .merge(pfname, how='left')
                       .merge(plname, how='left')
                       .merge(pmname, how='left'))
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
    fullnames = fullnames_clean(fullnames)
    return fullnames


def fullnames_clean(df):
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
    df = parens_clean(df)
    df = (df.fillna('')
            .groupby(['npi', 'pfname', 'pmname', 'plname'])
            .min()
            .reset_index())
    return df


def parens_clean(df):
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


def get_address(src, npis, entity, removaldate, entities, name_stub):
    """Is time varying, and exists for both entity types"""
    address = read_csv_npi(os.path.join(src, '%s.csv' % name_stub), npis)
    if 'name_stub' == 'ploctel':
        address['ploctel'] = (address.ploctel
                                     .astype('str')
                                     .str.split('.', expand=True)[0])
        address['ploctel'] = (address.ploctel.str.replace('-', '')
                                             .str.replace('(', '')
                                             .str.replace(')', '')
                                             .str.replace(' ', ''))
    address[name_stub] = address[name_stub].str.upper()
    if entities == 1 or entities == [1]:
        address = (address.merge(entity.query('entity==1'))
                          .drop(columns=['entity']))
    elif entities == 2 or entities == [2]:
        address = (address.merge(entity.query('entity==2'))
                          .drop(columns=['entity']))
    address = address.merge(removaldate, how='left', indicator=True)
    address['month'] = pd.to_datetime(address.month)
    address = address[(address.month <= address.npideactdate) |
                      (address.npideactdate.isnull())]
    address = address.drop(columns=['npideactdate', '_merge'])
    return address


def get_training_dates(src, entity):
    '''
    MD training details for all MDs and MD Students

    could add in discontinuities in location during training period
    would likely represent med school, internship, residency, fellowship
    '''

    taxcode = get_taxcode(src, None, entity, [1], temporal=True)
    mds = taxcode.query('cat=="MD/DO"').npi.drop_duplicates()
    studs = taxcode.query('cat=="MD/DO Student"').npi.drop_duplicates()
    fresh_mds = mds[mds.isin(studs)]
    old_mds = mds[~mds.isin(studs)]
    trainees = studs[~studs.isin(mds)]

    schools = sanitize_medical_schools()
    actually_freshmds = (schools.merge(trainees, how='right')
                                .dropna()
                                .query('grad_year<2016')
                                .npi
                                .drop_duplicates())
    fresh_mds = fresh_mds.append(actually_freshmds)
    trainees = trainees[~trainees.isin(actually_freshmds)]
    looklike_stilltrainee = (schools.merge(fresh_mds, how='right')
                                    .dropna()
                                    .query('grad_year>=2017')
                                    .npi.drop_duplicates())
    trainees = trainees.append(looklike_stilltrainee)
    fresh_mds = fresh_mds[~fresh_mds.isin(looklike_stilltrainee)]

    fresh_mds = taxcode.merge(fresh_mds)

    fresh_mds = (fresh_mds[['npi']]
                 .drop_duplicates()
                 .merge((fresh_mds.query('cat=="MD/DO Student"')
                        .drop(columns=['seq', 'ptaxcode', 'cat'])
                        .groupby('npi', as_index=False)
                        .first()
                        .rename(columns={'month': 'first_student_month'})),
                        how='left')
                 .merge((fresh_mds.query('cat=="MD/DO Student"')
                        .drop(columns=['seq', 'ptaxcode', 'cat'])
                        .groupby('npi', as_index=False)
                        .last()
                        .rename(columns={'month': 'last_student_month'})),
                        how='left')
                 .merge((fresh_mds.query('cat=="MD/DO"')
                        .drop(columns=['seq', 'ptaxcode', 'cat'])
                        .groupby('npi', as_index=False)
                        .first()
                        .rename(columns={'month': 'first_md_month'})),
                        how='left')
                 .merge((fresh_mds.query('cat=="MD/DO"')
                        .drop(columns=['seq', 'ptaxcode', 'cat'])
                        .groupby('npi', as_index=False)
                        .last()
                        .rename(columns={'month': 'last_md_month'})),
                        how='left'))
    fresh_mds = fresh_mds.merge(schools, how='left')
    trainee_dates = (taxcode.drop(columns=['seq', 'ptaxcode', 'cat'])
                            .drop_duplicates()
                            .merge(trainees)
                            .sort_values(['npi', 'month'])
                            .reset_index(drop=True))
    old_mds = pd.DataFrame(old_mds).merge(schools, how='left')
    training_details = {'trainees': trainee_dates,
                        'recent_mds': fresh_mds,
                        'older_mds': old_mds}
    return training_details


def sanitize_medical_schools(med_school_path=med_school_path,
                             fail_report=False):
    '''
    Note: this exercise uncovers the fact that there are some
    real MDs who are not using MD taxcodes.
    A more thorough fix would look at their credential string as well, and go
    back and make sure those were all crawled for schools as well
    Many of the below fails are actually podiatrists and chiropractors and
    others who apparently have schooling listed on their licenses. those
    are appropriate to throw out.
    '''
    schools = pd.read_csv(med_school_path)
    dups = schools.dropna()[schools.dropna().npi.duplicated()]
    schools = (schools[
        ~schools.npi.isin(dups.npi.drop_duplicates())].append(dups).dropna())
    schools.reset_index(drop=True, inplace=True)
    schools['grad_year'] = schools.grad_year.astype(int)
    assert schools.npi.is_unique

    # Real MDs:
    from .utils.globalcache import c
    npi = NPI()
    taxcode = c.get_taxcode(npi.src, None, npi.entity, [1])
    mds = (taxcode.query('cat == "MD/DO" or cat == "MD/DO Student"')
                  .npi.drop_duplicates())
    schools2 = schools.merge(mds, how='right', indicator=True)
    matches = (schools2.query('_merge=="both"')
                       .drop(columns='_merge')
                       .assign(grad_year=lambda x: x.grad_year.astype(int)))
    not_found = schools2.query('_merge=="right_only"').npi.drop_duplicates()
    rept = (schools.merge(matches, how='left', indicator=True)
                   .query('_merge!="both"')
                   .medical_school_upper
                   .value_counts())
    if fail_report:
        longprint(rept)
    return matches, not_found
