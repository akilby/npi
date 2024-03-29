import pandas as pd
from npi.npi import NPI, convert_practitioner_data_to_long, provider_taxonomies
from npi.pecos import PECOS
from npi.samhsa import SAMHSA
from npi.utils.utils import isid
from utils.loaders import pickle_read


def getcol(df, src, idvar, col, newname):
    return (df.merge(src[[idvar, col]].drop_duplicates())
              .rename(columns={col: newname}))


def conform_data_sources(source, cols, **kwargs):
    '''by default includes name, then adds
    other variables in a systematic fashion
    can use npi_source="ploc2" for secondary practice locations
    need to pass kwargs=practypes
    '''
    if isinstance(source, NPI):
        return conform_NPI(source, cols, **kwargs)
    elif isinstance(source, SAMHSA):
        return conform_SAMHSA(source, cols, **kwargs)
    elif isinstance(source, PECOS):
        return conform_PECOS(source, cols, **kwargs)


def conform_NPI(source, cols, **kwargs):
    df = source.expanded_fullnames.copy()
    idvar = 'npi'
    if 'practitioner_type' in cols:
        src = (source.practitioner_type
                     .pipe(convert_practitioner_data_to_long,
                           types=kwargs['practypes']))
        df = df.pipe(
            getcol, src, idvar, 'PractitionerType', 'practitioner_type')
    if 'state' in cols:
        if not kwargs or 'npi_source' not in kwargs.keys():
            src = (source
                   .plocstatename.drop(columns='month')
                   .drop_duplicates())
            df = df.pipe(getcol, src, idvar, 'plocstatename', 'state')
        elif 'npi_source' in kwargs.keys() and kwargs['npi_source'] == 'ploc2':
            src = (source
                   .secondary_practice_locations[[idvar, 'ploc2statename']]
                   .drop_duplicates())
            df = df.pipe(getcol, src, idvar, 'ploc2statename', 'state')
    if 'zip5' in cols:
        if not kwargs or 'npi_source' not in kwargs.keys():
            src = (source.ploczip
                         .assign(zip5=lambda x: x['ploczip'].str[:5])
                         .drop(columns=['month', 'ploczip'])
                         .drop_duplicates())
        elif 'npi_source' in kwargs.keys() and kwargs['npi_source'] == 'ploc2':
            src = (source
                   .secondary_practice_locations
                   .assign(
                    zip5=lambda x: x['ploc2zip'].str[:5])[[idvar, 'zip5']]
                   .drop_duplicates())
        df = df.pipe(getcol, src, idvar, 'zip5', 'zip5')
    if 'tel' in cols:
        if not kwargs or 'npi_source' not in kwargs.keys():
            src = (source
                   .ploctel.drop(columns='month')
                   .drop_duplicates())
            df = df.pipe(getcol, src, idvar, 'ploctel', 'tel')
        elif 'npi_source' in kwargs.keys() and kwargs['npi_source'] == 'ploc2':
            src = (source
                   .secondary_practice_locations[[idvar, 'ploc2tel']]
                   .drop_duplicates())
            src['tel'] = (src.ploc2tel
                             .astype('str')
                             .str.split('.', expand=True)[0])
            src['tel'] = (src.tel.str.replace('-', '')
                                     .str.replace('(', '')
                                     .str.replace(')', '')
                                     .str.replace(' ', ''))
            df = df.pipe(getcol, src, idvar, 'tel', 'tel')
    return df.drop_duplicates()


def conform_SAMHSA(source, cols, **kwargs):
    df = source.names.copy()
    idvar = 'samhsa_id'
    if 'practitioner_type' in cols:
        df = df.pipe(getcol, source.samhsa, idvar, 'PractitionerType',
                     'practitioner_type')
    if 'state' in cols:
        df = df.pipe(getcol, source.samhsa, idvar, 'State', 'state')
    if 'zip5' in cols:
        src = (source
               .samhsa
               .assign(zip5=lambda df: df['Zip'].str[:5])[[idvar, 'zip5']]
               .drop_duplicates())
        df = df.pipe(getcol, src, idvar, 'zip5', 'zip5')
    if 'tel' in cols:
        src = source.samhsa['samhsa_id']
        src2 = (pd.DataFrame(source.samhsa['Phone']
                                   .str.replace('-', '')
                                   .str.replace('(', '')
                                   .str.replace(')', '')
                                   .str.replace(' ', '')))
        src = pd.concat([src, src2], axis=1)
        df = df.pipe(getcol, src, idvar, 'Phone', 'tel')
    return df.drop_duplicates()


def conform_PECOS(source, cols, **kwargs):
    df = source.names.copy()
    idvar = 'NPI'
    if 'practitioner_type' in cols:
        df = df.pipe(getcol, source.practitioner_type, idvar, 'Credential',
                     'practitioner_type')
        df.loc[df.practitioner_type.isin(['MD', 'DO']),
               'practitioner_type'] = 'MD/DO'
        df.loc[df.practitioner_type.isin(['CNA']),
               'practitioner_type'] = 'CRNA'
        df = df[df.practitioner_type.isin(kwargs['practypes'])]
    if 'state' in cols:
        df = df.pipe(
            getcol, source.physician_compare, idvar, 'State', 'state')
    if 'zip5' in cols:
        src = (source
               .physician_compare
               .assign(zip5=lambda df: df['Zip Code'].astype(str).str[:5]))
        src = src[[idvar, 'zip5']].drop_duplicates()
        df = df.pipe(getcol, src, idvar, 'zip5', 'zip5')
    if 'tel' in cols:
        src = source.physician_compare['NPI']
        src2 = (source.physician_compare['Phone Number']
                      .astype('string')
                      .apply(lambda x: str(x).replace('.0', '')))
        src = pd.concat([src, src2], axis=1)
        df = df.pipe(getcol, pd.DataFrame(src), idvar, 'Phone Number', 'tel')
    return df.drop_duplicates()


def make_clean_matches(df1, df2, id_use, id_target,
                       blocklist=pd.DataFrame()):
    '''merges on common columns'''
    # DELETE IF NAME CONFLICTS IN MATCHES
    if not blocklist.empty:
        df1 = (df1.merge(blocklist, how='left', indicator=True)
                  .query('_merge=="left_only"'))[df1.columns]
        df2 = (df2.merge(blocklist, how='left', indicator=True)
                  .query('_merge=="left_only"'))[df2.columns]
    m = df1.merge(df2)[[id_use, id_target]].drop_duplicates()
    m = m[~m[id_use].duplicated(keep=False)]
    m = m[~m[id_target].duplicated(keep=False)]
    assert m[id_use].is_unique
    assert m[id_target].is_unique
    return m


def make_clean_matches_iterate(df1, idvar1, ordervar, df2, idvar2, blocklist):
    orders = sorted((df1[ordervar].value_counts().index.tolist()))
    for o in orders:
        m = make_clean_matches(
            df1.query(f'order=={o}'),
            df2,
            id_use=idvar1, id_target=idvar2,
            blocklist=blocklist[[x for x in blocklist.columns
                                 if x != 'order']])
        blocklist = blocklist.append(m.assign(order=o))
    return blocklist


def reconcat_names(df, firstname, middlename, lastname):
    n = (df.assign(
        n=lambda x: x[firstname] + ' ' + x[middlename] + ' ' + x[lastname])
           .n)
    df[f'{firstname}_r'] = n.apply(lambda y: y.split()[0])
    df[f'{middlename}_r'] = n.apply(lambda y: ' '.join(y.split()[1:-1]))
    df[f'{lastname}_r'] = n.apply(lambda y: y.split()[-1])
    return df


def generate_matches(s, npi, pecos, varlist, practypes, final_crosswalk):
    from npi.utils.globalcache import c
    df1 = conform_data_sources(s, varlist)
    df2 = conform_data_sources(npi, varlist, practypes=practypes)
    final_crosswalk = c.make_clean_matches_iterate(df1, 'samhsa_id', 'order',
                                                   df2, 'npi',
                                                   final_crosswalk)
    print('(1) Found %s matches' % final_crosswalk.shape[0])
    df3 = conform_data_sources(pecos, varlist, practypes=practypes)
    df3 = df3.rename(columns={'NPI': 'npi'})
    final_crosswalk = c.make_clean_matches_iterate(df1, 'samhsa_id', 'order',
                                                   df3, 'npi',
                                                   final_crosswalk)
    print('(2) Found %s matches' % final_crosswalk.shape[0])
    df4 = conform_data_sources(npi, varlist,
                               practypes=practypes, npi_source="ploc2")
    final_crosswalk = c.make_clean_matches_iterate(df1, 'samhsa_id', 'order',
                                                   df4, 'npi',
                                                   final_crosswalk)
    print('(3) Found %s matches' % final_crosswalk.shape[0])
    return final_crosswalk


# out = make_clean_matches_iterate(df1, 'samhsa_id', 'order', df2, 'npi', pd.DataFrame())
#
# priority_names = out[['samhsa_id']].assign(order=1).merge(df1)
# priority_names['new_firstname'] = priority_names.assign(n=lambda df: df['firstname'] + ' ' +  df['middlename'] + ' '  + df['lastname']).n.apply(lambda x: x.split()[0])
# priority_names['new_middlename'] = priority_names.assign(n=lambda df: df['firstname'] + ' ' +  df['middlename'] + ' '  + df['lastname']).n.apply(lambda x: ' '.join(x.split()[1:-1]))
# priority_names['new_lastname'] = priority_names.assign(n=lambda df: df['firstname'] + ' ' +  df['middlename'] + ' '  + df['lastname']).n.apply(lambda x: x.split()[-1])
# priority_names = priority_names.assign(new_suffix=lambda df: df.Suffix)
# priority_names = priority_names[['samhsa_id','new_firstname','new_middlename','new_lastname','new_suffix','practitioner_type','state','zip5']].drop_duplicates()
#
# # USE RECONCAT NAMES
# priority_names2 = out[['npi']].merge(df2)
# priority_names2['new_firstname'] = priority_names2.assign(n=lambda df: df['pfname'] + ' ' +  df['pmname'] + ' '  + df['plname']).n.apply(lambda x: x.split()[0])
# priority_names2['new_middlename'] = priority_names2.assign(n=lambda df: df['pfname'] + ' ' +  df['pmname'] + ' '  + df['plname']).n.apply(lambda x: ' '.join(x.split()[1:-1]))
# priority_names2['new_lastname'] = priority_names2.assign(n=lambda df: df['pfname'] + ' ' +  df['pmname'] + ' '  + df['plname']).n.apply(lambda x: x.split()[-1])
# priority_names2 = priority_names2.assign(new_suffix=lambda df: df.pnamesuffix)
# priority_names2 = priority_names2[['npi','new_firstname','new_middlename','new_lastname','new_suffix','practitioner_type','state','zip5']].drop_duplicates()
#
# expand_matches = out[['samhsa_id','npi']].merge(priority_names).merge(out[['samhsa_id','npi']].merge(priority_names2), how='outer', indicator=True)
# all_good = expand_matches.query('_merge=="both"')[['samhsa_id','npi']].drop_duplicates()
# expand_matches = expand_matches.merge(all_good, how='left', indicator='_merge2').query('_merge2!="both"').drop(columns='_merge2')
#
# o1 = out.merge(df1[['samhsa_id', 'middlename','Suffix']].dropna().query('middlename!="" or Suffix!=""').drop_duplicates())
# o2 = out.merge(df2[['npi', 'pmname', 'pnamesuffix']].dropna().query('pmname!="" or pnamesuffix!=""').drop_duplicates())
# lo1 = o1.merge(o2, left_on=o1.columns.tolist(), right_on=o2.columns.tolist(), how='outer', indicator=True).query('_merge=="left_only"')[['samhsa_id','npi']].drop_duplicates()
# ro1 = o1.merge(o2, left_on=o1.columns.tolist(), right_on=o2.columns.tolist(), how='outer', indicator=True).query('_merge=="right_only"')[['samhsa_id','npi']].drop_duplicates()
# lo1.merge(ro1)


def match_samhsa_npi():
    # I don't exploit timing here
    s = SAMHSA()
    s.retrieve('names')

    npi = NPI(entities=1)
    npi.retrieve('fullnames')
    npi.retrieve('expanded_fullnames')
    npi.retrieve('credentials')
    npi.retrieve('ptaxcode')
    npi.retrieve('practitioner_type')
    npi.retrieve('plocstatename')
    npi.retrieve('ploczip')
    npi.retrieve('ploctel')
    npi.retrieve('secondary_practice_locations')

    pecos = PECOS(['NPI', 'Last Name', 'First Name', 'Middle Name',
                   'Suffix', 'State', 'Zip Code', 'Phone Number'])
    pecos.retrieve('names')
    pecos.retrieve('practitioner_type')

    # matching data to generate a crosswalk
    final_crosswalk = pd.DataFrame()

    practypes = ['MD/DO', 'NP', 'PA', 'CRNA', 'CNM', 'CNS']

    # 0. TELEPHONE

    final_crosswalk = generate_matches(
        s, npi, pecos,
        ['practitioner_type', 'state', 'zip5', 'tel'],
        practypes, final_crosswalk)

    final_crosswalk = generate_matches(
        s, npi, pecos,
        ['practitioner_type', 'state', 'tel'],
        practypes, final_crosswalk)

    final_crosswalk = generate_matches(
        s, npi, pecos,
        ['practitioner_type', 'state', 'zip5'],
        practypes, final_crosswalk)

    final_crosswalk = generate_matches(
        s, npi, pecos,
        ['practitioner_type', 'state'],
        practypes, final_crosswalk)

    final_crosswalk = generate_matches(
        s, npi, pecos,
        ['state', 'zip5', 'tel'],
        practypes, final_crosswalk)

    final_crosswalk1 = generate_matches(
        s, npi, pecos,
        ['state', 'tel'],
        practypes, final_crosswalk)

    final_crosswalk2 = generate_matches(
        s, npi, pecos,
        ['state', 'zip5'],
        practypes, final_crosswalk)

    final_crosswalk3 = generate_matches(
        s, npi, pecos,
        ['state'],
        practypes, final_crosswalk)

    final_crosswalk4 = generate_matches(
        s, npi, pecos,
        ['practitioner_type'],
        practypes, final_crosswalk)

    final_crosswalk5 = generate_matches(
        s, npi, pecos,
        [],
        practypes, final_crosswalk)

    fin = (final_crosswalk1.merge(final_crosswalk, how='left', indicator=True)
                           .query('_merge=="left_only"'))
    fin = (fin.append(
        final_crosswalk2
        .merge(final_crosswalk, how='left', indicator=True)
        .query('_merge=="left_only"')))
    fin = fin.append(
        final_crosswalk3.query('order==1')
        .merge(s.names).query('middlename!=""')[['samhsa_id', 'npi']]
        .drop_duplicates()
        .merge(final_crosswalk, how='left', indicator=True)
        .query('_merge=="left_only"'))
    fin = fin.append(
        final_crosswalk4.query('order==1')
        .merge(s.names).query('middlename!=""')[['samhsa_id', 'npi']]
        .drop_duplicates()
        .merge(final_crosswalk, how='left', indicator=True)
        .query('_merge=="left_only"'))
    fin = fin.append(
        final_crosswalk5.query('order==1')
        .merge(s.names).query('middlename!=""')[['samhsa_id', 'npi']]
        .drop_duplicates()
        .merge(final_crosswalk, how='left', indicator=True)
        .query('_merge=="left_only"'))
    fin = fin[['samhsa_id', 'npi']].drop_duplicates()

    fin = fin[~fin['samhsa_id'].duplicated(keep=False)]
    fin = fin[~fin['npi'].duplicated(keep=False)]
    fin = final_crosswalk.append(fin).drop(columns='order').drop_duplicates()
    fin = fin.append(pd.DataFrame(dict(samhsa_id=[42325, 34010, 80, 62, 42387,
                                                  42333, 42339],
                                       npi=[1558332031, 1154652295,
                                            1871718890, 1275599524, 1457360588,
                                            1609002799, 1346518842]
                                       )))

    nopunct1 = (npi
                .expanded_fullnames
                .assign(nopunct=npi.expanded_fullnames['name']
                                   .str.replace("'", "")
                                   .str.replace('-', '')
                                   .str.replace(' ', ''))[['npi', 'nopunct']])
    remainders = (fin.merge(s.samhsa.drop_duplicates(),
                            how='right', on='samhsa_id', indicator=True)
                     .query('_merge=="right_only"'))
    nopunct2 = (remainders[['samhsa_id']]
                .merge(s.names)
                .assign(nopunct=lambda df: (df['name']
                                            .str.replace("'", "")
                                            .str.replace('-', '')
                                            .str.replace(' ', ''))))
    nopunct2 = nopunct2[['samhsa_id', 'nopunct']]
    matches = nopunct2.merge(nopunct1)
    matches2 = matches[['npi', 'samhsa_id']].drop_duplicates()
    matches2 = matches2[~matches2['samhsa_id'].duplicated(keep=False)]
    matches2 = matches2[~matches2['npi'].duplicated(keep=False)]
    newmatches = (matches2.merge(nopunct1)
                          .merge(nopunct2)[
                          matches2.merge(nopunct1)
                          .merge(nopunct2)
                          .nopunct.str.len() >= 10][['npi', 'samhsa_id']]
                  .drop_duplicates())
    newmatches = newmatches[~newmatches.samhsa_id.isin(fin.samhsa_id)]
    newmatches = newmatches[~newmatches.npi.isin(fin.npi)]
    fin = fin.append(newmatches)

    assert fin['samhsa_id'].is_unique
    assert fin['npi'].is_unique
    fin.reset_index(inplace=True, drop=True)
    return fin


def analysis_dataset():
    # some of this should get added to the PECOS class
    # including also the name match
    # Get matches of NPI to SAMHSA
    # matches = (pd.read_csv('/work/akilby/npi/final_matches.csv')
    #              .drop(columns='Unnamed: 0'))
    # from npi.utils.globalcache import c
    # matches = c.match_samhsa_npi()

    npi = NPI(entities=1)
    npi.retrieve('practitioner_type')
    npi_practype = (npi.practitioner_type
                       .pipe(convert_practitioner_data_to_long,
                             types=['MD/DO', 'NP', 'PA',
                                    'CRNA', 'CNM', 'CNS']))
    npi.retrieve('pgender')
    pecos = PECOS(['NPI', 'Last Name', 'First Name', 'Middle Name',
                   'Suffix', 'State', 'Zip Code', 'Phone Number'])
    pecos.retrieve('practitioner_type')

    # 1. Select MD/DO and NPs from either NPI or PECOS
    practitioners = (pecos.practitioner_type.merge(npi_practype,
                                                   how='left',
                                                   left_on="NPI",
                                                   right_on='npi'))
    mddo = (practitioners
            .query('Credential=="MD/DO" or Credential=="MD" or Credential=="DO'
                   '" or PractitionerType=="MD/DO"')
            .NPI.drop_duplicates())
    nps = practitioners.loc[(practitioners['Primary specialty']
                             == "NURSE PRACTITIONER")
                            | (practitioners['Credential'] == 'NP')
                            | (practitioners['PractitionerType'] == "NP")]
    nps = nps.NPI.drop_duplicates()

    # pecos_groups = PECOS(['NPI', 'Organization legal name',
    #                       'Group Practice PAC ID',
    #                       'Number of Group Practice members',
    #                       'Hospital affiliation CCN 1',
    #                       'Hospital affiliation LBN 1',
    #                       'Hospital affiliation CCN 2',
    #                       'Hospital affiliation LBN 2',
    #                       'Hospital affiliation CCN 3',
    #                       'Hospital affiliation LBN 3',
    #                       'Hospital affiliation CCN 4',
    #                       'Hospital affiliation LBN 4',
    #                       'Hospital affiliation CCN 5',
    #                       'Hospital affiliation LBN 5'],
    #                      drop_duplicates=False, date_var=True)

    # 2. Get group practice information. most sole practitioners
    # are missing a group practice ID
    pecos_groups_loc = PECOS(['NPI', 'Organization legal name',
                              'Group Practice PAC ID',
                              'Number of Group Practice members',
                              'State', 'Zip Code', 'Phone Number'],
                             drop_duplicates=False, date_var=True)

    groups = pecos_groups_loc.physician_compare.drop_duplicates()

    groups = groups.reset_index(drop=True).reset_index()
    # A bunch of sole practitioners (groupsize =1 ) are missing
    # give them a single-period group practice ID (not constant over
    # time even though other IDs are)
    groups.loc[
        groups['Group Practice PAC ID'].isnull(),
        'Group Practice PAC ID'] = (groups['index'] + 100000000000)
    groups = groups.drop(columns='index')
    groups = groups.merge(
        groups[['NPI', 'Group Practice PAC ID', 'date']]
        .drop_duplicates()
        .groupby(['Group Practice PAC ID', 'date'])
        .size()
        .reset_index())
    groups.loc[
        groups['Number of Group Practice members'].isnull(),
        'Number of Group Practice members'] = groups[0]
    groups.drop(columns=[0], inplace=True)

    coprac = (groups[['Group Practice PAC ID',
                      'Number of Group Practice members',
                      'State',
                      'Zip Code', 'date']]
              .drop_duplicates())

    coprac_ids = coprac.reset_index(drop=True).reset_index().rename(
        columns={'index': 'group_prac_zip_date_id'})
    coprac_np_counts = (groups
                        .merge(nps)
                        .merge(coprac_ids))
    idvars = ['group_prac_zip_date_id', 'date', 'NPI']
    coprac_np_counts = coprac_np_counts[idvars].drop_duplicates()
    coprac_np_counts = (coprac_np_counts
                        .groupby(['group_prac_zip_date_id', 'date'])
                        .size()
                        .reset_index()
                        .rename(columns={0: 'np_count'}))
    coprac_mds = (groups
                  .merge(mddo)
                  .merge(coprac_ids))
    coprac_mds = coprac_mds[idvars].drop_duplicates()
    coprac_mds = coprac_mds.merge(coprac_np_counts, how='left')
    coprac_mds['np_count'] = coprac_mds.np_count.fillna(0)
    preproc = (coprac_mds
               .sort_values(['NPI', 'date', 'np_count',
                             'group_prac_zip_date_id'])
               .groupby(['NPI', 'date']))
    mins = preproc.first()
    maxes = preproc.last()
    mins = (mins
            .reset_index()
            .merge(coprac_ids)
            .sort_values(['NPI', 'date'])
            .reset_index(drop=True))
    maxes = (maxes
             .reset_index()
             .merge(coprac_ids)
             .sort_values(['NPI', 'date'])
             .reset_index(drop=True))

    copracs = mins.merge(maxes, on=['NPI', 'date'], suffixes=['_min', '_max'])
    # mins = (coprac_mds
    #         .drop(columns='group_prac_zip_date_id')
    #         .groupby(['NPI', 'date'], as_index=False).min())
    # maxes = (coprac_mds.drop(columns='group_prac_zip_date_id')
    #                    .groupby(['NPI', 'date'], as_index=False).max())
    # copracs = mins.merge(maxes.rename(columns={'np_count': 'np_count_max'}))
    assert (copracs[['NPI', 'date']].drop_duplicates().shape[0]
            == copracs.shape[0])
    # Specialties. time varying?
    pecos_specs = PECOS(['NPI', 'Primary specialty',
                         'Secondary specialty 1',
                         'Secondary specialty 2',
                         'Secondary specialty 3',
                         'Secondary specialty 4'],
                        drop_duplicates=False, date_var=True)

    mddo = pecos_specs.physician_compare.merge(mddo)
    prim_spec = mddo[['NPI', 'date', 'Primary specialty']].drop_duplicates()
    prim_spec = prim_spec.groupby(['NPI', 'date']).first().reset_index()
    # prim_spec = pd.concat([m[['NPI', 'date']],
    #                        pd.get_dummies(
    #                         m['Primary specialty'])],
    #                       axis=1).groupby(['NPI', 'date']).sum()
    # prim_spec = 1*(prim_spec > 0)

    sec_spec = (mddo.drop(columns=['Primary specialty'])
                    .drop_duplicates()[mddo.drop(columns=['Primary specialty'])
                                           .drop_duplicates()
                                           .isnull().sum(1) < 4]
                    .set_index(['NPI', 'date'])
                    .stack()
                    .reset_index()
                    .drop(columns='level_2')
                    .dropna()
                    .drop_duplicates()
                    .rename(columns={0: 'secondary_spec'})
                    .query('secondary_spec!=" "'))
    sec_spec = pd.concat([sec_spec[['NPI', 'date']],
                          pd.get_dummies(
                          sec_spec['secondary_spec'])],
                         axis=1).groupby(['NPI', 'date']).sum()
    sec_spec = 1*(sec_spec > 0)

    copracs = copracs.merge(prim_spec)
    # copracs = copracs.merge(sec_spec, how='left')
    copracs = copracs.merge(sec_spec.reset_index(), how='left')
    copracs = copracs.fillna({x: 0 for x in sec_spec.columns})
    # copracs = copracs.merge(mddo[['NPI', 'Primary specialty']])
    pecos_education = PECOS(['NPI', 'Medical school name', 'Graduation year'])
    copracs = (copracs
               .merge(pecos_education
                      .physician_compare[['NPI', 'Graduation year']]
                      .groupby('NPI', as_index=False)
                      .first()))
    copracs['gradyear'] = pd.qcut(copracs['Graduation year'], 20)
    copracs = copracs.merge(npi.pgender, left_on='NPI', right_on='npi')

    # waiver dates from new file

    matches = pickle_read(
        '/work/akilby/npi/Cache/Caches/output_1588990540883395.pkl')

    s = SAMHSA()
    samhsa_match = (s.samhsa[['WaiverType', 'samhsa_id', 'Date']]
                     .drop_duplicates())
    samhsa_match = samhsa_match.merge(matches)
    sam = (samhsa_match[['npi', 'Date', 'WaiverType']]
           .groupby(['npi', 'WaiverType'])
           .min()
           .unstack(1)
           .reset_index())
    sam.columns = ['npi', 'Date30', 'Date100', 'Date275']

    copracs = copracs.merge(sam, how='left')
    copracs = copracs.drop(columns=['NPI', 'Graduation year'])
    for variable in ['Group Practice PAC ID_min',
                     'Group Practice PAC ID_max',
                     'Number of Group Practice members_min',
                     'Number of Group Practice members_max']:
        copracs[variable] = copracs[variable].astype(int)
    copracs['State_min'] = copracs['State_min'].astype(str)
    copracs['State_max'] = copracs['State_max'].astype(str)
    copracs['Zip Code_min'] = copracs['Zip Code_min'].astype(str)
    copracs['Zip Code_max'] = copracs['Zip Code_max'].astype(str)
    copracs['Primary specialty'] = copracs['Primary specialty'].astype(str)
    isid(copracs, ['npi', 'date'])
    return copracs


def final_analysis_dataset(final):
    npi = NPI(entities=1)

    # gender
    npi.retrieve('pgender')

    # education
    educ = (PECOS(['NPI', 'Medical school name', 'Graduation year'])
            .physician_compare)
    educ = educ.groupby('NPI', as_index=False).first()
    educ['gradyear'] = pd.qcut(educ['Graduation year'], 20)

    # Specialties. time varying?
    pecos_specs = PECOS(['NPI', 'Primary specialty',
                         'Secondary specialty 1',
                         'Secondary specialty 2',
                         'Secondary specialty 3',
                         'Secondary specialty 4'],
                        drop_duplicates=False, date_var=True)

    specs = (pecos_specs
             .physician_compare
             .drop(columns='date')
             .drop_duplicates()
             .set_index(['NPI', 'Primary specialty'])
             .stack()
             .reset_index()
             .drop(columns='level_2')
             .rename(columns={0: 'secondary_spec'}))

    npi.retrieve('ptaxcode')
    taxcodes = npi.ptaxcode[['npi', 'ptaxcode']]

    # waiver dates from new file
    matches = pickle_read(
      '/work/akilby/npi/Cache/Caches/output_1588990540883395.pkl')
    s = SAMHSA()
    samhsa_match = (s.samhsa[['WaiverType', 'samhsa_id', 'Date', 'State']]
                    .drop_duplicates())
    samhsa_match = samhsa_match.merge(matches)
    sam2 = (samhsa_match[['npi', 'State', 'Date', 'WaiverType']]
            .groupby(['npi', 'State', 'WaiverType'])
            .min()
            .unstack(2)
            .reset_index())
    sam2.columns = ['npi', 'State', 'Date30', 'Date100', 'Date275']

    # ####

    final = final.reset_index().merge(educ, how='left')
    final = final.rename(columns={'NPI': 'npi'}).merge(npi.pgender)

    # because I added in the state column, I now lose 884 Mds
    final = final.merge(sam2, how='left')

    specs = (specs
             .set_index('NPI')
             .stack()
             .reset_index()
             .drop(columns='level_1')
             .drop_duplicates()
             .rename(columns={0: 'spec', 'NPI': 'npi'})
             .query('spec!=" "'))
    specs = specs.merge(final.npi.drop_duplicates())

    taxcodes = taxcodes.merge(final.npi.drop_duplicates())
    tax_desc = (taxcodes
                .merge(provider_taxonomies(),
                       left_on='ptaxcode', right_on='TaxonomyCode')
                [['npi', 'ptaxcode', 'Classification', 'Specialization']]
                .set_index(['npi', 'ptaxcode'])
                .stack()
                .reset_index()
                .drop(columns='level_2')
                .rename(columns={0: 'spec'})
                .assign(spec=lambda df: df.spec.str.upper()))
    tax_desc.loc[lambda df: df.spec.str.endswith(' -'),
                 'spec'] = (tax_desc
                            .loc[lambda df: df.spec.str.endswith(' -')]
                            .spec.str.replace(' -', ''))
    tax_desc = (tax_desc[~((tax_desc
                            .ptaxcode
                            .isin(provider_taxonomies()
                                  .query('Specialization=="General Practice"')
                                  .TaxonomyCode) &
                            (tax_desc.spec == 'GENERAL PRACTICE')))])

    allspec = specs.append(tax_desc.drop(columns='ptaxcode')).drop_duplicates()
    allspec.loc[lambda df: df.spec == 'PREVENTATIVE MEDICINE',
                'spec']='PREVENTIVE MEDICINE'
    allspec = allspec.assign(spec=lambda df: df.spec.str.strip())
    # t = (taxcodes
    #      .merge(final[~final.npi.isin(specs.npi)].npi.drop_duplicates())
    #      .rename(columns={'ptaxcode': 'spec'}))
    # t = specs.append(t)

    # new
    # taxes = (t.merge(provider_taxonomies(),
    #                  left_on='spec', right_on='TaxonomyCode')
    #          [['npi', 'spec', 'Classification']]
    #          .drop_duplicates()
    #          .dropna()
    #          .append(t.merge(provider_taxonomies(),
    #                  left_on='spec', right_on='TaxonomyCode')
    #                   .query('Specialization!="General Practice"')
    #                  [['npi', 'spec', 'Specialization']]
    #                  .drop_duplicates()
    #                  .dropna()
    #                  .rename(columns={'Specialization': 'Classification'}))
    #          .sort_values(['npi', 'spec', 'Classification'])
    #          .assign(spec=lambda df: df.Classification.str.upper().str.strip())
    #          )
    # taxes.loc[lambda df: df.spec.str.endswith(' -'),
    #           'spec'] = (taxes
    #                      .loc[lambda df: df.spec.str.endswith(' -')]
    #                      .spec.str.replace(' -', ''))
    # taxes = taxes.drop(columns='Classification')
    # taxes = (t
    #          .merge(provider_taxonomies(),
    #                 how='left', left_on='spec',
    #                 right_on='TaxonomyCode', indicator=True)
    #          .query('_merge=="left_only"')[['npi', 'spec']]
    #          .append(taxes)
    #          .drop_duplicates())
    # t = taxes.copy()

    t2 = allspec[allspec.spec.isin(allspec.spec.value_counts()[
         allspec.spec.value_counts() > 500].index)]
    t2 = (pd.concat([t2, pd.get_dummies(t2.spec)], axis=1)
            .drop(columns='spec').groupby('npi').sum())
    t2 = (t2
          .reset_index()
          .merge(final.npi.drop_duplicates(), how='right')
          .fillna(0))
    return final, allspec, t2


def make_samhsa_waiver_analysis_dataset():
    from npi.utils.globalcache import c
    groupinfo, missinggroup = c.group_practices_info_infer()
    group_inferred, group_count1, group_count2 = c.group_practices_impute(
        groupinfo, missinggroup)
    mds_nps, practypes = c.mds_nps_practype_npi_pecos()
    locdata = c.make_master_enrollee_dataframe(mds_nps)
    (group_inferred_q_all, group_count3,
        pecos_dates, all_dates) = c.infer_all_group_practices(
        group_inferred, locdata)
    counts = c.get_md_np_counts(group_inferred_q_all, practypes)
    final = c.md_copractices(counts, locdata, practypes)
    final, specs_long, specializations = c.final_analysis_dataset(final)
    enumdates, all_dates, earliest_date = c.get_useful_enrollment_dates(
        final, all_dates)
    final = c.get_urbrurs(group_inferred_q_all, final)
    return (final, specs_long, specializations,
            enumdates, all_dates, earliest_date)


    # copracs.to_stata('/work/akilby/Analysis/samhsa_master_analysis_data2.dta',
    #                  write_index=False)


    # df1 = conform_data_sources(
    #     s, ['practitioner_type', 'state', 'zip5', 'tel'])
    # df2 = conform_data_sources(
    #     npi, ['practitioner_type', 'state', 'zip5', 'tel'],
    #     practypes=practypes)
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
    # df3 = conform_data_sources(pecos,
    #                            ['practitioner_type', 'state', 'zip5', 'tel'],
    #                            practypes=practypes)
    # df3 = df3.rename(columns={'NPI': 'npi'})
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df3, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # # 1. exact match on name, practitioner type, state, and zip code
    # df1 = conform_data_sources(s, ['practitioner_type', 'state', 'zip5'])
    # df2 = conform_data_sources(npi, ['practitioner_type', 'state', 'zip5'],
    #                            practypes=practypes)
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
    # df3 = conform_data_sources(pecos, ['practitioner_type', 'state', 'zip5'],
    #                            practypes=practypes)
    # df3 = df3.rename(columns={'NPI': 'npi'})
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df3, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # # 2. exact match on name, practitioner type, and state
    # df1 = conform_data_sources(s, ['practitioner_type', 'state'])
    # df2 = conform_data_sources(npi, ['practitioner_type', 'state'],
    #                            practypes=practypes)
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # # 3. exact match on name, practitioner type, and secondary practice state
    # # and zip code
    # df1 = conform_data_sources(s, ['practitioner_type', 'state', 'zip5'])
    # df2 = conform_data_sources(npi, ['practitioner_type', 'state', 'zip5'],
    #                            practypes=practypes, npi_source="ploc2")
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # # 4. exact match on name, practitioner type, and secondary practice state
    # df1 = conform_data_sources(s, ['practitioner_type', 'state'])
    # df2 = conform_data_sources(npi, ['practitioner_type', 'state'],
    #                            practypes=practypes, npi_source="ploc2")
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # # 5. PECOS: exact match on name, type, state, and zip
    # df1 = conform_data_sources(s, ['practitioner_type', 'state', 'zip5'])
    # df2 = conform_data_sources(pecos, ['practitioner_type', 'state', 'zip5'],
    #                            practypes=practypes)
    # df2 = df2.rename(columns={'NPI': 'npi'})
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # # 6. PECOS: exact match on name, type, state
    # df1 = conform_data_sources(s, ['practitioner_type', 'state'])
    # df2 = conform_data_sources(pecos, ['practitioner_type', 'state'],
    #                            practypes=practypes)
    # df2 = df2.rename(columns={'NPI': 'npi'})
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # # 7. exact match on name, practitioner type, in NPI
    # df1 = conform_data_sources(s, ['practitioner_type'])
    # df2 = conform_data_sources(npi, ['practitioner_type'],
    #                            practypes=practypes)
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # # 8. PECOS: exact match on name, type
    # df1 = conform_data_sources(s, ['practitioner_type'])
    # df2 = conform_data_sources(pecos, ['practitioner_type'],
    #                            practypes=practypes)
    # df2 = df2.rename(columns={'NPI': 'npi'})
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # assert final_crosswalk.samhsa_id.is_unique
    # assert final_crosswalk.npi.is_unique
#
    # # 9. exact match on name, state, and zip code
    # df1 = conform_data_sources(s, ['state', 'zip5'])
    # df2 = conform_data_sources(npi, ['state', 'zip5'])
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # # 10. PECOS: exact match on name, state, and zip
    # df1 = conform_data_sources(s, ['state', 'zip5'])
    # df2 = conform_data_sources(pecos, ['state', 'zip5'],)
    # df2 = df2.rename(columns={'NPI': 'npi'})
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # # 11. exact match on name, state
    # df1 = conform_data_sources(s, ['state'])
    # df2 = conform_data_sources(npi, ['state'])
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])
#
    # # 12. PECOS: exact match on name, state
    # df1 = conform_data_sources(s, ['state'])
    # df2 = conform_data_sources(pecos, ['state'],)
    # df2 = df2.rename(columns={'NPI': 'npi'})
    # final_crosswalk = make_clean_matches_iterate(df1, 'samhsa_id', 'order',
    #                                              df2, 'npi', final_crosswalk)
    # print('Found %s matches' % final_crosswalk.shape[0])

    # ## REMOVE CONFLICTING MIDDLE NAMES AND SUFFIXES

# ADD SUFFIXES!
# suffixes = ['JR', 'III', 'II', 'SR', 'IV']
# # First: match to NPI database
# npi_names_credential_state_zip = (
#     npi.expanded_fullnames
#        .merge(npi.practitioner_type.pipe(convert_practitioner_data_to_long))
#        .merge(npi.plocstatename.drop(columns='month').drop_duplicates())
#        .merge(npi.ploczip
#               .assign(zip5=lambda df: df['ploczip'].str[:5])
#               .drop(columns=['month', 'ploczip'])
#               .drop_duplicates())
#        )





# Next, drop zipcode and match only on state, credential and name
# for o in orders:
#     m = make_clean_matches(
#         samhsa_names_credential_state_zip.query(f'order=={o}')
#                                          .drop(columns='zip5'),
#         npi_names_credential_state_zip.drop(columns='zip5'),
#         id_use='samhsa_id', id_target='npi',
#         blocklist=final_crosswalk)
#     final_crosswalk = final_crosswalk.append(m)


# Next, secondary practice locations


# Next, try PECOS


# col_rename = {'NPI': 'npi',
#               'Last Name': 'lastname',
#               'Middle Name': 'middlename',
#               'First Name': 'firstname',
#               'State': 'plocstatename'}
# pc_names_state_zip = (
#     pc.rename(columns=col_rename)
#       .assign(zip5=lambda df: df['Zip Code'].astype(str).str[:5])
#       .drop(columns=['Zip Code', 'Suffix'])
#       )
# names = pc_names_state_zip.drop(columns=['plocstatename', 'zip5'])
# names = names.assign(**{x: names[x].fillna('').astype(str)
#                         for x in names.columns if x != 'npi'})
# names = (names.pipe(expand_names_in_sensible_ways,
#                     idvar='npi',
#                     firstname='firstname',
#                     middlename='middlename',
#                     lastname='lastname')
#               .drop(columns='name')
#               .drop_duplicates())
#
# pc_names_credential_state_zip = (
#     names.merge(pc_names_state_zip.drop(columns=['lastname', 'firstname',
#                                                  'middlename']))
#          .merge(npi.practitioner_type.pipe(convert_practitioner_data_to_long)))
#
# for o in orders:
#     m = make_clean_matches(
#         samhsa_names_credential_state_zip.query(f'order=={o}'),
#         pc_names_credential_state_zip,
#         id_use='samhsa_id', id_target='npi',
#         blocklist=final_crosswalk)
#     final_crosswalk = final_crosswalk.append(m)
#
# # Next, drop zipcode and match only on state, credential and name
# for o in orders:
#     m = make_clean_matches(
#         samhsa_names_credential_state_zip.query(f'order=={o}')
#                                          .drop(columns='zip5'),
#         pc_names_credential_state_zip.drop(columns='zip5'),
#         id_use='samhsa_id', id_target='npi',
#         blocklist=final_crosswalk)
#     final_crosswalk = final_crosswalk.append(m)
#
# ##############################################################################
# ##############################################################################
# ##############################################################################
# ##############################################################################
#
# # merges in all variations of names with all possible states that NPI is
# # observed
# # at
# new = (npi.expanded_fullnames
#           .merge((npi.ptaxcode
#                      .dropna()
#                      .assign(practype=lambda x:
#                              x.cat.str.replace(' Student', ''))
#                      .drop(columns=['cat', 'ptaxcode'])
#                      .drop_duplicates()), how='left')
#           .merge((npi.plocstatename[['npi', 'plocstatename']]
#                      .drop_duplicates()), how='left')
#           .drop(columns='entity'))
#
# npi.secondary_practice_locations
#
#
# q = 'pcredential_stripped=="MD" or pcredential_stripped=="DO"'
#
# new = (new.append((new.merge(new.loc[new.practype == "MD/DO"][['npi']]
#                                 .drop_duplicates()
#                                 .merge(npi.credentials
#                                           .query(q)[['npi']]
#                                           .drop_duplicates(),
#                                        how='outer', indicator=True)
#                                 .query('_merge=="right_only"')
#                                 .drop(columns='_merge'))
#                       .fillna(value={'practype': 'MD/DO'})
#                       .append((new.merge(
#                         new.loc[new.practype == "MD/DO"][['npi']]
#                            .drop_duplicates()
#                            .merge(npi.credentials.query(q)[['npi']]
#                                                  .drop_duplicates(),
#                                   how='outer', indicator=True)
#                            .query('_merge=="right_only"')
#                            .drop(columns='_merge'))
#                                   .assign(practype='MD/DO')))
#                       .drop_duplicates()))
#           .drop_duplicates())
#
#
# sam = s.names.merge(s.samhsa[['PractitionerType', 'State', 'samhsa_id']].drop_duplicates())
# samhsa_matches = sam.drop(columns=['firstname', 'middlename', 'lastname', 'Credential String']).merge(new.drop(columns=['pfname','pmname','plname']), left_on=['name','PractitionerType','State'], right_on=['name','practype','plocstatename'])
# samhsa_matches = samhsa_matches.merge(samhsa_matches[['samhsa_id', 'npi']].drop_duplicates().groupby('samhsa_id').count().reset_index().query('npi>1').drop(columns='npi'), on='samhsa_id', indicator=True, how='outer').query('_merge=="left_only"').drop(columns=['_merge'])
# samhsa_matches = samhsa_matches[~samhsa_matches.npi.isin(samhsa_matches[['samhsa_id', 'npi']].drop_duplicates().groupby('npi').count().query('samhsa_id>1').reset_index().npi)]
#
# matches1 = samhsa_matches[['samhsa_id', 'npi']].drop_duplicates()
# remainders1 = sam.merge(samhsa_matches[['samhsa_id']].drop_duplicates(), how='outer', indicator=True).query('_merge!="both"').drop(columns='_merge')
#
# remainders1 = remainders1.query('middlename!=""').merge(new.query('pmname!=""'), left_on=['name','PractitionerType','State'], right_on=['name','practype','plocstatename']).sort_values('samhsa_id')
# matches2 = remainders1[~remainders1.samhsa_id.isin(remainders1[['samhsa_id', 'npi']].drop_duplicates().groupby('samhsa_id').count().query('npi>1').reset_index().samhsa_id)]
# matches2 = matches2[['samhsa_id', 'npi']].drop_duplicates()
#
# matches = matches1.append(matches2)
#
# dups = matches[['npi']][matches[['npi']].duplicated()]
# dd = s.samhsa.drop(columns='npi').merge(matches[matches.npi.isin(dups.npi)], on='samhsa_id').sort_values('npi').drop(columns=['Unnamed: 0', 'LocatorWebsiteConsent','First name', 'Last name','NPI state','Date', 'Telephone', 'statecode', 'geoid', 'zcta5', 'zip', 'CurrentPatientLimit', 'NumberPatientsCertifiedFor', 'Index', 'DateGranted', 'Street2'])
# dd[['County']] = dd.County.str.upper()
# dd[['City']] = dd.City.str.upper()
#
# a1 = dd[~dd.npi.isin(dd[['npi','PractitionerType','Phone']].drop_duplicates()[dd[['npi','PractitionerType','Phone']].drop_duplicates().npi.duplicated()].npi)][['samhsa_id', 'npi']]
# a2 = dd[~dd.npi.isin(dd[['npi','PractitionerType','County']].drop_duplicates()[dd[['npi','PractitionerType','County']].drop_duplicates().npi.duplicated()].npi)][['samhsa_id', 'npi']]
# a3 = dd[~dd.npi.isin(dd[['npi','PractitionerType','City', 'State']].drop_duplicates()[dd[['npi','PractitionerType','City', 'State']].drop_duplicates().npi.duplicated()].npi)][['samhsa_id', 'npi']]
# fine = a1.append(a2).append(a3).drop_duplicates()
# dups[~dups.npi.isin(fine.npi)]
#
# matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]
# remainders2 = sam.merge(matches, how='outer', indicator=True).query('_merge!="both"').drop(columns=['_merge', 'npi'])
#
# zips = npi.loczip[['npi', 'ploczip']].drop_duplicates()
# zips['zip']=zips.ploczip.str[:5]
# zips = zips[['npi','zip']].drop_duplicates()
# new2 = new.drop(columns='plocstatename').drop_duplicates().merge(zips)
#
# sam2 = s.names.merge(s.samhsa[['PractitionerType', 'Zip', 'samhsa_id']].drop_duplicates())
# sam2['zip'] = sam2.Zip.str[:5]
# sam2 = sam2.drop(columns="Zip").drop_duplicates()
# sam2 = sam2.merge(remainders2[['samhsa_id']].drop_duplicates())
# new_matches = sam2.drop(columns=['firstname', 'middlename', 'lastname', 'Credential String']).merge(new2.drop(columns=['pfname','pmname','plname']), left_on=['name','PractitionerType','zip'], right_on=['name','practype','zip'])
# matches = matches.append(new_matches[['samhsa_id', 'npi']].drop_duplicates())
# dups = matches[['npi']][matches[['npi']].duplicated()]
# matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]
#
# remainders3 = sam.merge(matches, how='outer', indicator=True).query('_merge!="both"').drop(columns=['_merge', 'npi'])
#
#
# new3 = new2[['npi', 'name', 'practype']].drop_duplicates()
# match4 = remainders3[['name', 'PractitionerType', 'samhsa_id']].drop_duplicates().merge(new3, left_on=['name','PractitionerType'], right_on=['name','practype'])
# match4 = match4[['samhsa_id', 'npi']].drop_duplicates()
# match4 = match4[~match4.samhsa_id.isin(match4[match4.samhsa_id.duplicated()].samhsa_id.drop_duplicates())]
# matches = matches.append(match4[['samhsa_id', 'npi']].drop_duplicates())
# dups = matches[['npi']][matches[['npi']].duplicated()]
# matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]
# remainders4 = sam.merge(matches, how='outer', indicator=True).query('_merge!="both"').drop(columns=['_merge', 'npi'])
#
#
# new4 = new2.query('pmname!=""')[['npi', 'name', 'practype']].drop_duplicates()
# match5 = remainders4.query('middlename!=""')[['name','PractitionerType','samhsa_id']].drop_duplicates().merge(new4, left_on=['name','PractitionerType'], right_on=['name','practype'])
# match5 = match5[['samhsa_id', 'npi']].drop_duplicates()
# match5 = match5[~match5.samhsa_id.isin(match5[match5.samhsa_id.duplicated()].samhsa_id.drop_duplicates())]
# matches = matches.append(match5[['samhsa_id', 'npi']].drop_duplicates())
# dups = matches[['npi']][matches[['npi']].duplicated()]
# matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]
# remainders5 = sam.merge(matches, how='outer', indicator=True).query('_merge!="both"').drop(columns=['_merge', 'npi'])
#
#
# remainders5['name_nochars']=remainders5['name'].str.replace("'"," ").str.replace('-',' ').str.replace('.',' ')
# new2['name_nochars']=new2['name'].str.replace("'"," ").str.replace('-',' ').str.replace('.',' ')
# new['name_nochars']=new['name'].str.replace("'"," ").str.replace('-',' ').str.replace('.',' ')
#
# sam_zips = s.names.merge(s.samhsa[['PractitionerType', 'Zip', 'samhsa_id']].drop_duplicates())
# sam_zips['zip'] = sam_zips.Zip.str[:5]
# sam_zips = sam_zips.drop(columns="Zip").drop_duplicates()
#
# a1 = remainders5.merge(new, left_on=['name_nochars', 'PractitionerType','State'], right_on=['name_nochars','practype','plocstatename'])
# a2 = remainders5.merge(new, left_on=['name_nochars', 'PractitionerType','State'], right_on=['name','practype','plocstatename'])
# a3 = remainders5.merge(sam_zips, how='left').merge(new2, left_on=['name_nochars', 'PractitionerType','zip'], right_on=['name_nochars','practype','zip'])
# a4 = remainders5.merge(sam_zips, how='left').merge(new2, left_on=['name_nochars', 'PractitionerType','zip'], right_on=['name','practype','zip'])
#
# newmatched_a = a3.append(a4)[['samhsa_id', 'npi']].drop_duplicates()
# dups = newmatched_a[['npi']][newmatched_a[['npi']].duplicated()]
# dups2 = newmatched_a[['samhsa_id']][newmatched_a[['samhsa_id']].duplicated()]
# newmatched_a = newmatched_a[~newmatched_a.samhsa_id.isin(dups2.samhsa_id)]
# newmatched_a = newmatched_a[~newmatched_a.npi.isin(dups.npi)]
# newmatched_a.drop_duplicates()
# newmatched_a.samhsa_id.drop_duplicates()
# newmatched_a.npi.drop_duplicates()
# matches = matches.append(newmatched_a[['samhsa_id', 'npi']].drop_duplicates())
# dups = matches[['npi']][matches[['npi']].duplicated()]
# matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]
#
# newmatched_b = a1.append(a2)[['samhsa_id', 'npi']].drop_duplicates()
# newmatched_b = newmatched_b.merge(newmatched_a['samhsa_id'], indicator=True, how='left').query('_merge!="both"').drop(columns='_merge')
# dups = newmatched_b[['npi']][newmatched_b[['npi']].duplicated()]
# dups2 = newmatched_b[['samhsa_id']][newmatched_b[['samhsa_id']].duplicated()]
# newmatched_b = newmatched_b[~newmatched_b.samhsa_id.isin(dups2.samhsa_id)]
# newmatched_b = newmatched_b[~newmatched_b.npi.isin(dups.npi)]
# newmatched_b.drop_duplicates()
# newmatched_b.samhsa_id.drop_duplicates()
# newmatched_b.npi.drop_duplicates()
# matches = matches.append(newmatched_b[['samhsa_id', 'npi']].drop_duplicates())
# dups = matches[['npi']][matches[['npi']].duplicated()]
# matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]
#
# remainders6 = sam.merge(matches, how='outer', indicator=True).query('_merge!="both"').drop(columns=['_merge', 'npi'])
# remainders6['name_nochars']=remainders6['name'].str.replace("'","").str.replace('-','').str.replace('.','')
# new2['name_nochars']=new2['name'].str.replace("'","").str.replace('-',' ').str.replace('.','')
# new['name_nochars']=new['name'].str.replace("'","").str.replace('-',' ').str.replace('.','')
# a1 = remainders6.merge(new, left_on=['name_nochars', 'PractitionerType','State'], right_on=['name_nochars','practype','plocstatename'])
# a2 = remainders6.merge(new, left_on=['name_nochars', 'PractitionerType','State'], right_on=['name','practype','plocstatename'])
# a3 = remainders6.merge(sam_zips, how='left').merge(new2, left_on=['name_nochars', 'PractitionerType','zip'], right_on=['name_nochars','practype','zip'])
# a4 = remainders6.merge(sam_zips, how='left').merge(new2, left_on=['name_nochars', 'PractitionerType','zip'], right_on=['name','practype','zip'])
# newmatched_a = a3.append(a4)[['samhsa_id', 'npi']].drop_duplicates()
# dups = newmatched_a[['npi']][newmatched_a[['npi']].duplicated()]
# dups2 = newmatched_a[['samhsa_id']][newmatched_a[['samhsa_id']].duplicated()]
# newmatched_a = newmatched_a[~newmatched_a.samhsa_id.isin(dups2.samhsa_id)]
# newmatched_a = newmatched_a[~newmatched_a.npi.isin(dups.npi)]
# newmatched_a.drop_duplicates()
# newmatched_a.samhsa_id.drop_duplicates()
# newmatched_a.npi.drop_duplicates()
# matches = matches.append(newmatched_a[['samhsa_id', 'npi']].drop_duplicates())
# dups = matches[['npi']][matches[['npi']].duplicated()]
# matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]
#
# newmatched_b = a1.append(a2)[['samhsa_id', 'npi']].drop_duplicates()
# newmatched_b = newmatched_b.merge(newmatched_a['samhsa_id'], indicator=True, how='left').query('_merge!="both"').drop(columns='_merge')
# dups = newmatched_b[['npi']][newmatched_b[['npi']].duplicated()]
# dups2 = newmatched_b[['samhsa_id']][newmatched_b[['samhsa_id']].duplicated()]
# newmatched_b = newmatched_b[~newmatched_b.samhsa_id.isin(dups2.samhsa_id)]
# newmatched_b = newmatched_b[~newmatched_b.npi.isin(dups.npi)]
# newmatched_b.drop_duplicates()
# newmatched_b.samhsa_id.drop_duplicates()
# newmatched_b.npi.drop_duplicates()
# matches = matches.append(newmatched_b[['samhsa_id', 'npi']].drop_duplicates())
# dups = matches[['npi']][matches[['npi']].duplicated()]
# matches = matches[~matches.npi.isin(dups[~dups.npi.isin(fine.npi)].npi)]
#
# matches = matches.append(pd.DataFrame({'samhsa_id': [3796, 3795, 214], 'npi': [1699731661, 1720044811, 1770933707]})).drop_duplicates().sort_values('samhsa_id').reset_index(drop=True)
#
# remainders7 = sam.merge(matches, how='outer', indicator=True).query('_merge!="both"').drop(columns=['_merge', 'npi']).merge(sam_zips, how='left')
# matches.to_csv('/work/akilby/npi/samhsa_npi_partial_match.csv')
# usable_data = s.samhsa.drop(columns='npi').merge(matches, on='samhsa_id').sort_values('npi').drop(columns=['Unnamed: 0', 'LocatorWebsiteConsent','First name', 'Last name','NPI state','Date',  'statecode', 'geoid', 'zcta5', 'zip', 'Index'])
#
# usable_data = usable_data.drop_duplicates().reset_index(drop=True)
#
# usable_data['Date']=usable_data['DateGranted']
# usable_data.loc[usable_data['Date'].isnull(), 'Date'] = usable_data['DateLastCertified']
#
# usable_data.to_csv('/work/akilby/npi/samhsa_npi_usable_data.csv', index=False)
#
# #these all actually look correct
# matches[matches.samhsa_id.isin(matches[matches.samhsa_id.duplicated()].samhsa_id)].head(60)
#
# # check for special characters
#

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





# name_matches = expanded_fullnames[['name','npi']].merge(expanded_names[['name','samhsa_id']])
#
#
# # taxcode.merge(credentials, how='outer')
#
# samhsa.merge(npi.fullnames[['npi','name']], left_on='NameFull', right_on='name').merge(npi.credentials, how='left', on='npi').merge(npi.taxcode, how='left', on='npi')


# def check_in(it, li):
#     return it in li
#
#
# def replace_end(namestr, stub):
#     return (namestr.replace(stub, '').strip() if namestr.endswith(stub)
#             else namestr.strip())
#
#
# # for b in badl:
# #     df_samhsa['NameFull'] = df_samhsa.NameFull.apply(lambda x: replace_end(x, b))
#
#
# def npi_names_to_samhsa_matches(firstname, lastname, df_samhsa):
#     u = df_samhsa[df_samhsa.NameFull.str.contains(lastname)]
#     u = u[u.NameFull.str.split(lastname).str[0].str.contains(firstname)]
#     if not u.empty:
#         u = u.reset_index(drop=True)
#         u['splitli'] = u.NameFull.str.split(' ')
#         u = u[u.splitli.apply(lambda x: check_in(firstname, x))]
#         u = u[u.splitli.apply(lambda x: check_in(lastname, x))]
#     return u
#
#
# def state_npis(stateabbrev, states, names):
#     npis = states.query(
#         'plocstatename=="%s"' % stateabbrev).npi.drop_duplicates()
#     return names.merge(npis)
#
#
# def npi_names_sahmsa_matches_statebatch(stateabbrev):
#     s = time.time()
#     outcome_df, fn, ln = pd.DataFrame(), [], []
#     searchdf = df_samhsa.query('State=="%s"' % stateabbrev)
#     use_df = state_npis(stateabbrev, states, df)
#     print('number of rows: %s' % use_df.shape[0])
#     for i, row in use_df.iterrows():
#         if round(i/10000) == i/10000:
#             print(i)
#         npi = row['npi']
#         lastname = row['plname']
#         firstname = row['pfname']
#         try:
#             o = npi_names_to_samhsa_matches(firstname, lastname, searchdf)
#         except re.error:
#             fn.append(firstname)
#             ln.append(lastname)
#         if not o.empty:
#             o['npi'] = npi
#             o['pfname'] = firstname
#             o['plname'] = lastname
#             outcome_df = outcome_df.append(o)
#     print('time:', time.time() - s)
#     return outcome_df, fn, ln
#
#
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


### WORKING WITH PECOS ###

# src = '/work/akilby/npi/samhsa_processing/FOIA_12312019_datefilled_clean_NPITelefill.csv'
# samhsa = pd.read_csv(src, low_memory=False)
# idvars = ['NameFull', 'DateLastCertified', 'PractitionerType']
# for idvar in idvars:
#     samhsa[idvar] = samhsa[idvar].str.upper()
# ids = (samhsa[idvars].drop_duplicates()
#                      .reset_index(drop=True)
#                      .reset_index()
#                      .rename(columns=dict(index='samhsa_id')))
# samhsa_orig = samhsa.merge(ids)
#
# samhsa_match = pd.read_csv('/work/akilby/npi/samhsa_processing/samhsa_npi_usable_data_with_locs.csv')
#
#
# samhsa_orig.samhsa_id.drop_duplicates()
# samhsa_match.samhsa_id.drop_duplicates()
#
# cols = ['Primary specialty','Secondary specialty 1', 'Secondary specialty 2','Secondary specialty 3', 'Secondary specialty 4']
# groups = physician_compare_select_vars(['Group Practice PAC ID', 'Number of Group Practice members']+cols, drop_duplicates=False,date_var=True)
# samhsa_match = samhsa_match[['WaiverType', 'PractitionerType', 'samhsa_id', 'npi', 'location_no', 'ploctel', 'zip', 'plocstatename', 'Date']].drop_duplicates()
#
# npi=NPI(entities=1)
# npi.retrieve('ptaxcode')
#
#
# groups.assign(prim=groups['Primary specialty']).query('prim=="NURSE PRACTITIONER"').groupby(['Group Practice PAC ID', 'date']).size()
# mds_pecos=npi.ptaxcode.query('cat=="MD/DO" or cat=="MD/DO Student"')[['npi', 'cat']].assign(cat2=lambda df: df.cat=="MD/DO").groupby('npi', as_index=False).sum().merge(groups.rename(columns={'NPI':'npi'}))
#
#
# badli = ['PHYSICIAN ASSISTANT',
#          'CERTIFIED REGISTERED NURSE ANESTHETIST',
#          'CLINICAL SOCIAL WORKER',
#          'PODIATRY',
#          'NURSE PRACTITIONER',
#          'CLINICAL PSYCHOLOGIST',
#          'CERTIFIED REGISTERED NURSE ANESTHETIST (CRNA)',
#          'OPTOMETRY',
#          'CHIROPRACTIC',
#          'PSYCHOLOGIST, CLINICAL',
#          'OCCUPATIONAL THERAPY',
#          'CERTIFIED NURSE MIDWIFE',
#          'REGISTERED DIETITIAN OR NUTRITION PROFESSIONAL',
#          'CERTIFIED NURSE MIDWIFE (CNM)']
#
#
# for item in badli:
#     mds_pecos.loc[(mds_pecos.cat2==0) & mds_pecos['Primary specialty'].notnull() & (mds_pecos['Primary specialty']==item), 'bad'] = 1
#
# mds_pecos=mds_pecos.query('bad!=1')
# mds_pecos=mds_pecos.drop(columns=['cat2','bad'])
#
#
# group_np_count=groups[['Group Practice PAC ID', 'date']].drop_duplicates().dropna().merge(groups.assign(prim=groups['Primary specialty']).query('prim=="NURSE PRACTITIONER"').groupby(['Group Practice PAC ID', 'date']).size().reset_index().rename(columns={0: 'NPCount'}), how='left').fillna(0).sort_values(['Group Practice PAC ID', 'date']).reset_index(drop=True)
#
# mds_pecos.merge(group_np_count, how='left').assign(NPCount=lambda df: df['NPCount'].fillna(0).astype(int))
#
# s=samhsa_match[['npi','Date','WaiverType']].groupby(['npi','WaiverType']).min().unstack(1).reset_index()
# s.columns=['npi','Date30','Date100','Date275']
#
# npc = npcounts.reset_index().merge(s, how='left').sort_values(['npi','date']).assign(has_30=lambda df: 1*((df.Date30.notnull()) & (df.date>=pd.to_datetime(df.Date30))),
#                                                                                      has_100=lambda df: 1*((df.Date100.notnull()) & (df.date>=pd.to_datetime(df.Date100))),
#                                                                                      has_275=lambda df: 1*((df.Date275.notnull()) & (df.date>=pd.to_datetime(df.Date275))))
#

    # samhsa_match_names = final_crosswalk.drop(columns='order').merge(s.names).query('order==1')
    # samhsa_middle_init = samhsa_match_names[samhsa_match_names.middlename.str.len() == 1]
    # samhsa_middle_name = samhsa_match_names[samhsa_match_names.middlename.str.len() > 1]
    # samhsa_no_middle_init = samhsa_match_names[samhsa_match_names.middlename.str.len() == 0]
#
    # npi_match_names = final_crosswalk.drop(columns='order').merge(npi.expanded_fullnames).pipe(reconcat_names, 'pfname', 'pmname', 'plname')
    # npi_middle_init = npi_match_names[npi_match_names.pmname_r.str.len() == 1]
    # npi_middle_name = npi_match_names[npi_match_names.pmname_r.str.len() > 1]
#
    # pnr = pecos.names.rename(columns={'NPI': 'npi', 'First Name': 'FirstName', 'Middle Name': 'MiddleName', 'Last Name': 'LastName'})
    # pecos_match_names = final_crosswalk.drop(columns='order').merge(pnr).pipe(reconcat_names, 'FirstName', 'MiddleName', 'LastName')
    # pecos_middle_init = pecos_match_names[pecos_match_names['MiddleName_r'].str.len() == 1]
    # pecos_middle_name = pecos_match_names[pecos_match_names['MiddleName_r'].str.len() > 1]
#
    # m1 = samhsa_middle_init.merge(npi_middle_init, how='outer', on=['samhsa_id','npi'], indicator=True).query('_merge=="both" and pmname_r==middlename')[['samhsa_id','npi']].drop_duplicates()
    # m2 = samhsa_middle_init.merge(pecos_middle_init, how='outer', on=['samhsa_id','npi'], indicator=True).query('_merge=="both" and MiddleName_r==middlename')[['samhsa_id','npi']].drop_duplicates()
#
    # nom1 = samhsa_middle_init.merge(npi_middle_init, how='outer', on=['samhsa_id','npi'], indicator=True).query('_merge=="both" and pmname_r!=middlename')[['samhsa_id','npi']].drop_duplicates()
    # nom2 = samhsa_middle_init.merge(pecos_middle_init, how='outer', on=['samhsa_id','npi'], indicator=True).query('_merge=="both" and MiddleName_r!=middlename')[['samhsa_id','npi']].drop_duplicates()

    # maybe only keep for early orders

    # *** remove conflicts from order 1 using reconcat here -- or maybe not
