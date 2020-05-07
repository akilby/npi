"""
Notes: for names, and fullnames, only for entity=1
Also, stored at the individual, not individual-month level

For addresses, should really add the other practice addresses (ploc2) that got
added to the NPI in recent years in different files
"""

import os
import re
from functools import reduce

import pandas as pd
from npi.constants import USE_VAR_LIST_DICT
# from download.medical_schools import sanitize_web_medical_schools
from utility_data.taxonomies import provider_taxonomies

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

    def display_gettable_attributes(self):
        fc = [x.replace('get_', '') for x in dir(self) if x.startswith('get_')]
        raw = [x for x in fc if x in USE_VAR_LIST_DICT.keys()]
        proc = [x for x in fc if x not in USE_VAR_LIST_DICT.keys()]
        print('Gettable raw data:\n', raw, '\n\n')
        print('Gettable processed data:\n', proc)

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

    def get_pnamesuffix(self):
        if hasattr(self, 'pnamesuffix') or self.entities in [2, [2]]:
            return
        from .utils.globalcache import c
        name = c.get_name(self.src, self.npis, self.entity, 'pnamesuffix')
        name['pnamesuffix'] = name.pnamesuffix.str.replace('.', '')
        self.pnamesuffix = name

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

    def get_pnamesuffixoth(self):
        if hasattr(self, 'pnamesuffixoth') or self.entities in [2, [2]]:
            return
        from .utils.globalcache import c
        nameoth = c.get_nameoth(
            self.src, self.npis, self.entity, 'pnamesuffixoth')
        nameoth['pnamesuffixoth'] = nameoth.pnamesuffixoth.str.replace('.', '')
        self.pnamesuffixoth = nameoth

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

    def get_PLICSTATE(self):
        if hasattr(self, 'PLICSTATE'):
            return
        self.get_removaldate()
        from .utils.globalcache import c
        self.PLICSTATE = c.get_lic(
            self.src, self.npis, self.entity,
            self.removaldate, 'PLICSTATE', temporal=True)

    def get_PLICNUM(self):
        if hasattr(self, 'PLICNUM'):
            return
        self.get_removaldate()
        from .utils.globalcache import c
        self.PLICNUM = c.get_lic(
            self.src, self.npis, self.entity,
            self.removaldate, 'PLICNUM', temporal=True)

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
                               'ploctel')
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
        credentials = credentials.merge(credentials_map(),
                                        left_on='pcredential_stripped',
                                        right_on='credential',
                                        how='left').drop(columns='credential')
        self.credentials = credentials

    def get_licenses(self):
        if hasattr(self, 'licenses') or self.entities in [2, [2]]:
            return
        self.get_removaldate()
        from .utils.globalcache import c
        self.licenses = c.get_licenses(
            self.src, self.npis, self.entity, self.removaldate)

    def get_fullnames(self):
        if hasattr(self, 'fullnames') or self.entities in [2, [2]]:
            return
        self.get_pfname()
        self.get_pmname()
        self.get_plname()
        self.get_pnamesuffix()
        self.get_pfnameoth()
        self.get_pmnameoth()
        self.get_plnameoth()
        self.get_pnamesuffixoth()

        from .utils.globalcache import c
        self.fullnames = c.get_fullnames(self.pfname,
                                         self.pmname,
                                         self.plname,
                                         self.pnamesuffix,
                                         self.pfnameoth,
                                         self.pmnameoth,
                                         self.plnameoth,
                                         self.pnamesuffixoth)

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
        (firstname, middlename, lastname, suffix) = ('pfname',
                                                     'pmname',
                                                     'plname',
                                                     'pnamesuffix')
        self.expanded_fullnames = expand_names_in_sensible_ways(
            f, idvar, firstname, middlename, lastname, suffix,
            handle_suffixes_in_lastname=True)

    def get_secondary_practice_locations(self):
        if hasattr(self, 'secondary_practice_locations'):
            return
        dfs = [pd.read_csv(os.path.join(self.src, f'ploc2{item}.csv'))
               for item in
               ['line1', 'line2', 'cityname', 'statename', 'tel', 'zip']]
        dfm = reduce(lambda left, right: pd.merge(left, right), dfs)
        self.secondary_practice_locations = dfm

    def get_practitioner_type(self):
        if hasattr(self, 'practitioner_type') or self.entities in [2, [2]]:
            return

        self.get_credentials()
        self.get_ptaxcode()

        credentials_orig = self.credentials
        taxcode_orig = self.ptaxcode.query('entity==1')

        # gets rid of student codes if we can identify what
        # a person later becomes using credentials or taxcodes.
        # If not, they are marked as student
        credentials = pd.concat(
            [credentials_orig.npi, pd.get_dummies(credentials_orig.cat,
                                                  dummy_na=True)],
            axis=1).groupby('npi').max()
        credentials.columns = credentials.columns.fillna('No Category')
        taxcode = pd.concat(
            [taxcode_orig.npi, pd.get_dummies(taxcode_orig.cat,
                                              dummy_na=True)],
            axis=1).groupby('npi').max()
        taxcode.columns = taxcode.columns.fillna('No Category')

        prac_type = (credentials.reset_index()
                                .merge(taxcode.reset_index(), how='outer')
                                .fillna(0)
                                .groupby('npi').max())

        prac_type = prac_type.assign(su=(prac_type[[x for x
                                                    in prac_type.columns
                                                    if x != "No Category"
                                                    and x != "Student"]]
                                         ).sum(1))
        prac_type.loc[prac_type.su >= 1, 'Student'] = 0
        prac_type.loc[prac_type.su >= 1, 'No Category'] = 0
        prac_type = prac_type.drop(columns='su')
        for col in prac_type.columns:
            prac_type[col] = prac_type[col].astype(int)

        self.practitioner_type = prac_type.reset_index()


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


def handle_suffixes_in_lastnames(df, lastname_col, suffix_col,
                                 suffixes, suffixes_neverend):
    temp_col = f'{lastname_col}_final'
    df[temp_col] = df[lastname_col]

    def handle_suffix(df, s):
        fstrings = [f', {s}', f' {s}', f',{s}']
        if s in suffixes_neverend:
            fstrings = fstrings + [f'{s}']
        for fstr in fstrings:
            df.loc[df[lastname_col].str.endswith(fstr), suffix_col] = s
            df = df.assign(l2=lambda x: x[lastname_col].str.split(fstr).str[0])
            df.loc[df[lastname_col].str.endswith(fstr), temp_col] = df.l2
        return df.drop(columns='l2')

    for s in suffixes:
        df = handle_suffix(df, s)

    df[temp_col] = df[temp_col].apply(
        lambda x: x[:-1] if x.endswith(',') else x)
    df[temp_col] = df[temp_col].apply(
        lambda x: x[:-2] if x.endswith(', ') else x)
    df[temp_col] = df[temp_col].str.strip()
    df = df.drop(columns=lastname_col).rename(columns={temp_col: lastname_col})
    return df


def expand_names_in_sensible_ways(df, idvar, firstname, middlename, lastname,
                                  suffix=None,
                                  handle_suffixes_in_lastname=False):
    '''
    For custom fuzzy matching
    '''
    from .utils.globalcache import c
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

    # dealing with suffixes in lastnames
    suffixes = [x for x
                in expanded_full[suffix].value_counts().index
                if x != '']
    suffixes = suffixes + [' '.join(list(x)) for x in suffixes]
    suffixes_neverend = ['JR', 'III', 'VIII']

    if suffix and handle_suffixes_in_lastname:
        expanded_full = c.handle_suffixes_in_lastnames(
            expanded_full, lastname, suffix, suffixes, suffixes_neverend)

    # turn into one name column
    expanded_full.loc[expanded_full[middlename] == '', 'name'] = (
        expanded_full[firstname] + ' ' + expanded_full[lastname])
    expanded_full.loc[expanded_full[middlename] != '', 'name'] = (
        expanded_full[firstname] + ' ' + expanded_full[middlename]
        + ' ' + expanded_full[lastname])
    if suffix:
        suff = expanded_full.query('%s!=""' % suffix).copy()
        suff2 = expanded_full.query('%s!=""' % suffix).copy()
        suff[suffix] = ''
        suff2['name'] = suff2['name'] + " " + suff2[suffix]
        expanded_full = (expanded_full.query('%s==""' % suffix)
                                      .append(suff)
                                      .append(suff2))
    k = [idvar, firstname, middlename, lastname]
    if suffix:
        k = k + [suffix]
    k = k + ['name']
    expanded_full = expanded_full[k].drop_duplicates().reset_index(drop=True)
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
    crna = (tax.query('Classification == "Nurse Anesthetist, '
                      'Certified Registered"')
               .TaxonomyCode
               .tolist())
    cnm = (tax.query('Classification == "Advanced Practice Midwife"')
              .TaxonomyCode
              .tolist())
    cns = (tax.query('Classification == "Clinical Nurse Specialist"')
              .TaxonomyCode
              .tolist())
    c = tax.query('Classification=="Chiropractor"').TaxonomyCode.tolist()
    d = tax.query('Classification=="Dentist"').TaxonomyCode.tolist()
    po = tax.query('Classification=="Podiatrist"').TaxonomyCode.tolist()
    ph = tax.query('Classification=="Pharmacist"').TaxonomyCode.tolist()
    o = tax.query('Classification=="Optometrist"').TaxonomyCode.tolist()
    p = tax.query('Classification=="Psychologist"').TaxonomyCode.tolist()
    student = tax.query('TaxonomyCode=="390200000X"').TaxonomyCode.tolist()

    df.loc[(df.ptaxcode.isin(pa) & df.entity == 1), 'cat'] = 'PA'
    df.loc[(df.ptaxcode.isin(np) & df.entity == 1), 'cat'] = 'NP'
    df.loc[(df.ptaxcode.isin(mddo) & df.entity == 1), 'cat'] = 'MD/DO'
    df.loc[(df.ptaxcode.isin(crna) & df.entity == 1), 'cat'] = 'CRNA'
    df.loc[(df.ptaxcode.isin(cnm) & df.entity == 1), 'cat'] = 'CNM'
    df.loc[(df.ptaxcode.isin(cns) & df.entity == 1), 'cat'] = 'CNS'

    df.loc[(df.ptaxcode.isin(c) & df.entity == 1), 'cat'] = 'Chiropractor'
    df.loc[(df.ptaxcode.isin(d) & df.entity == 1), 'cat'] = 'Dentist'
    df.loc[(df.ptaxcode.isin(po) & df.entity == 1), 'cat'] = 'Podiatrist'
    df.loc[(df.ptaxcode.isin(ph) & df.entity == 1), 'cat'] = 'Pharmacist'
    df.loc[(df.ptaxcode.isin(o) & df.entity == 1), 'cat'] = 'Optometrist'
    df.loc[(df.ptaxcode.isin(p) & df.entity == 1), 'cat'] = 'Psychologist'

    df.loc[(df.ptaxcode.isin(student) & df.entity == 1), 'cat'] = 'Student'
    return df


def get_taxcode(src, npis, entity, entities, temporal=False):
    """
    Retrieves taxonomy codes (including all 15 entries if necessary)
    Entity type 1 or 2 can have a taxcode
    Returns non-temporal data unless otherwise specified;
    all taxcodes associated with a given NPI
    Assigns a category only to entity types 1
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
            return taxcode_all
        elif entities in [2, [2]]:
            return taxcode_all.query('entity==2')


def get_lic(src, npis, entity, removaldate, name_stub, temporal=False):
    """
    Retrieves PLICSTATE, PLICNUM
    """
    lic = read_csv_npi(os.path.join(src, f'{name_stub}.csv'), npis)
    if not temporal:
        lic = lic[['npi', name_stub]].drop_duplicates()
    else:
        lic = lic.merge(removaldate, how='left', indicator=True)
        lic['month'] = pd.to_datetime(lic.month)
        lic = lic[(lic.month <= lic.npideactdate) |
                  (lic.npideactdate.isnull())]
        lic = lic.drop(columns=['npideactdate', '_merge'])

    return lic


def get_licenses(src, npis, entity, removaldate):
    from .utils.globalcache import c
    licstate = c.get_lic(
        src, npis, entity, removaldate, 'PLICSTATE', temporal=True)
    licnum = c.get_lic(
        src, npis, entity, removaldate, 'PLICNUM', temporal=True)
    df = licstate.merge(licnum, how='outer')
    return df[['npi', 'PLICNUM', 'PLICSTATE']].drop_duplicates()


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


def get_fullnames(pfname, pmname, plname, pnamesuffix,
                  pfnameoth, pmnameoth, plnameoth, pnamesuffixoth):
    name_list = ['pfname', 'pmname', 'plname', 'pnamesuffix']
    oth_list = ['pfnameoth', 'pmnameoth', 'plnameoth', 'pnamesuffixoth']
    ren = {'plnameoth': 'plname',
           'pfnameoth': 'pfname',
           'pmnameoth': 'pmname',
           'pnamesuffixoth': 'pnamesuffix'}

    fullnames = pd.merge(pfname, plname, how='outer')
    fullnames = pd.merge(fullnames, pmname, how='outer')
    fullnames = pd.merge(fullnames, pnamesuffix, how='outer')
    fullnames = fullnames[['npi'] + name_list]
    merged = (pfnameoth.merge(plnameoth, how='outer')
                       .merge(pmnameoth, how='outer')
                       .merge(pnamesuffixoth, how='outer')
                       .merge(pfname, how='left')
                       .merge(plname, how='left')
                       .merge(pmname, how='left')
                       .merge(pnamesuffix, how='left'))
    merged.loc[merged.pfnameoth.isnull(), 'pfnameoth'] = merged.pfname
    merged.loc[merged.plnameoth.isnull(), 'plnameoth'] = merged.plname

    merged2 = merged.copy()
    merged.loc[merged.pmnameoth.isnull(), 'pmnameoth'] = merged.pmname
    merged.loc[merged.pmnameoth.isnull(),
               'pnamesuffixoth'] = merged.pnamesuffix

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
    merged = merged.assign(othflag=1)
    fullnames = fullnames.assign(othflag=0)
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


def credentials_map(return_type='DataFrame'):
    """
    This is obviously not complete. Currently focused on the credentials that
    show up in the SAMHSA data.
    """
    assert return_type in ['dict', 'DataFrame']
    d = {'MD/DO': ['MD', 'DO'],
         'PA': ['PA-C', 'PA', 'PAC', 'PHYSICIANASSISTANT', 'RPA-C', 'MPAS'],
         'NP': ['NP', 'FNP', 'ARNP', 'FNP-C', 'CRNP', 'FNP-BC', 'CNP',
                'NP-C', 'DNP', 'CPNP'],
         'Other APRN': ['APN', 'APRN'],
         'CRNA': ['CRNA'],
         'CNM': ['CNM'],
         'CNS': ['CNS'],
         'Chiropractor': ['DC'],
         'Dentist': ['DDS', 'DMD'],
         'Podiatrist': ['DPM'],
         'Pharmacist': ['PHARMD', 'RPH', 'PHARMACIST', 'DPH', 'BCPS'],
         'Optometrist': ['OD'],
         'Psychologist': ['PSYD', 'PSYCHOLOGIST']
         }
    if return_type == 'dict':
        return d
    else:
        return (pd.DataFrame.from_dict(d, orient='index')
                            .stack()
                            .reset_index()
                            .drop(columns='level_1')
                            .rename(columns={'level_0': 'cat',
                                             0: 'credential'})[['credential',
                                                                'cat']])


def credential_taxonomy_classification_pairs(credentials,
                                             taxonomies,
                                             search_credential=None,
                                             search_taxonomy=None):
    tax = provider_taxonomies()
    if search_taxonomy:
        s = pd.DataFrame({'Classification': [search_taxonomy]})
        return (taxonomies.merge(tax.merge(s),
                                 left_on='ptaxcode',
                                 right_on='TaxonomyCode')[['npi']]
                          .drop_duplicates()
                          .merge(credentials)
                          .pcredential_stripped.value_counts())
    elif search_credential:
        s = pd.DataFrame({'pcredential_stripped': [search_credential]})
        return (credentials.merge(s)[['npi']]
                           .drop_duplicates()
                           .merge(taxonomies)[['ptaxcode']]
                           .merge(tax, left_on='ptaxcode',
                                  right_on='TaxonomyCode')
                           .Classification.value_counts())
    else:
        raise Exception('cannot do both search_credential and search_taxonomy')


def convert_practitioner_data_to_long(df,
                                      colname="PractitionerType",
                                      types=['MD/DO', 'NP', 'PA']):
    return (df.set_index('npi')
              .stack()
              .rename('indic')
              .reset_index()
              .query('indic==1')
              .rename(columns={'level_1': colname})
              .drop(columns='indic')
              .merge(pd.DataFrame({colname: types}))
            )


#def get_training_dates(src, entity):
#     '''
#     This is extremely rough and should get replaced
#     MD training details for all MDs and MD Students
# 
#     could add in discontinuities in location during training period
#     would likely represent med school, internship, residency, fellowship
#     '''
# 
#     taxcode = get_taxcode(src, None, entity, [1], temporal=True)
#     mds = taxcode.query('cat=="MD/DO"').npi.drop_duplicates()
#     studs = taxcode.query('cat=="MD/DO Student"').npi.drop_duplicates()
#     fresh_mds = mds[mds.isin(studs)]
#     old_mds = mds[~mds.isin(studs)]
#     trainees = studs[~studs.isin(mds)]
# 
#     schools = sanitize_web_medical_schools()
#     actually_freshmds = (schools.merge(trainees, how='right')
#                                 .dropna()
#                                 .query('grad_year<2016')
#                                 .npi
#                                 .drop_duplicates())
#     fresh_mds = fresh_mds.append(actually_freshmds)
#     trainees = trainees[~trainees.isin(actually_freshmds)]
#     looklike_stilltrainee = (schools.merge(fresh_mds, how='right')
#                                     .dropna()
#                                     .query('grad_year>=2017')
#                                     .npi.drop_duplicates())
#     trainees = trainees.append(looklike_stilltrainee)
#     fresh_mds = fresh_mds[~fresh_mds.isin(looklike_stilltrainee)]
# 
#     fresh_mds = taxcode.merge(fresh_mds)
# 
#     fresh_mds = (fresh_mds[['npi']]
#                  .drop_duplicates()
#                  .merge((fresh_mds.query('cat=="MD/DO Student"')
#                         .drop(columns=['seq', 'ptaxcode', 'cat'])
#                         .groupby('npi', as_index=False)
#                         .first()
#                         .rename(columns={'month': 'first_student_month'})),
#                         how='left')
#                  .merge((fresh_mds.query('cat=="MD/DO Student"')
#                         .drop(columns=['seq', 'ptaxcode', 'cat'])
#                         .groupby('npi', as_index=False)
#                         .last()
#                         .rename(columns={'month': 'last_student_month'})),
#                         how='left')
#                  .merge((fresh_mds.query('cat=="MD/DO"')
#                         .drop(columns=['seq', 'ptaxcode', 'cat'])
#                         .groupby('npi', as_index=False)
#                         .first()
#                         .rename(columns={'month': 'first_md_month'})),
#                         how='left')
#                  .merge((fresh_mds.query('cat=="MD/DO"')
#                         .drop(columns=['seq', 'ptaxcode', 'cat'])
#                         .groupby('npi', as_index=False)
#                         .last()
#                         .rename(columns={'month': 'last_md_month'})),
#                         how='left'))
#     fresh_mds = fresh_mds.merge(schools, how='left')
#     trainee_dates = (taxcode.drop(columns=['seq', 'ptaxcode', 'cat'])
#                             .drop_duplicates()
#                             .merge(trainees)
#                             .sort_values(['npi', 'month'])
#                             .reset_index(drop=True))
#     old_mds = pd.DataFrame(old_mds).merge(schools, how='left')
#     training_details = {'trainees': trainee_dates,
#                         'recent_mds': fresh_mds,
#                         'older_mds': old_mds}
#     return training_details
# 
