"""

elig:
drop if old grad year, or if they drop out after high historical claims (if
only a few claims, maybe cant determine)"
people can move to private practice..."

could also try; retire them when they hit retirement age, unless evidence
they still practice

"""

import numpy as np
import pandas as pd
from npi.npi import NPI

from .download.medical_schools import final_data_path as med_school_path
from .npi import expand_names_in_sensible_ways
from .process.medicare import part_b_files, part_d_files
# from .process.physician_compare import physician_compare_select_vars
from .utils.utils import isid


class PECOS(object):
    def __init__(self, init_vars=[], drop_duplicates=True, date_var=False):
        from .utils.globalcache import c
        if init_vars:
            self.physician_compare = c.physician_compare_select_vars(
                init_vars, drop_duplicates, date_var)

    def retrieve(self, thing):
        getattr(self, f'get_{thing}')()

    def get_names(self):
        if hasattr(self, 'names'):
            return
        from .utils.globalcache import c
        cols = ['NPI', 'Last Name', 'First Name', 'Middle Name', 'Suffix']
        if hasattr(self, 'physician_compare'):
            cols2 = [x for x in cols
                     if x not in self.physician_compare.columns]
        if cols2:
            varl = list(set(['NPI'] + cols2))
            pc = c.physician_compare_select_vars(varl)
            pc = self.physician_compare.merge(pc)
        else:
            pc = self.physician_compare

        pc = pc.assign(**{x: pc[x].astype(object)
                                  .fillna('').astype(str).str.strip()
                          for x in pc.columns if x != 'NPI'})
        pc['Suffix'] = pc['Suffix'].str.replace('.', '')
        pc = pc[cols].drop_duplicates()
        names = (pc.pipe(expand_names_in_sensible_ways,
                         idvar='NPI',
                         firstname='First Name',
                         middlename='Middle Name',
                         lastname='Last Name',
                         suffix='Suffix',
                         handle_suffixes_in_lastname=True))
        self.names = names

    def get_practitioner_type(self):
        """Gets credentials and primary specialty. Primary specialty is much
        more complete
        than credentials, so I check that specialty maps to credentials
        and impute
        credentials from specialty. Can also be verified in the NPI. Note
        in imputation I can't distinguish between MD and DO."""
        if hasattr(self, 'practitioner_type'):
            return
        from .utils.globalcache import c
        cols = ['NPI', 'Credential', 'Primary specialty']
        if hasattr(self, 'physician_compare'):
            cols2 = [x for x in cols
                     if x not in self.physician_compare.columns]
        if cols2:
            varl = ['NPI'] + cols2 if 'NPI' not in cols2 else cols2
            pc = c.physician_compare_select_vars(varl)
        else:
            pc = self.physician_compare[varl]
        pc2 = (pc.drop(columns='NPI').dropna()
                 .query('Credential!=" "').reset_index(drop=True))
        pc2.loc[pc2['Credential'].isin(['MD', 'DO']), 'Credential'] = 'MD/DO'
        pc2 = (pc2.groupby(['Primary specialty', 'Credential'])
                  .size().reset_index()
                  .merge(pc2.groupby(['Primary specialty', 'Credential'])
                            .size().groupby(level=0).max().reset_index()))
        mapping = (pc2.rename(columns={0: 'count'})
                      .query('count>150').drop(columns='count'))
        pc3 = pc.merge(mapping, on='Primary specialty')
        pc3.loc[pc3.Credential_x.isnull() &
                pc3.Credential_y.notnull(), 'Credential_x'] = pc3.Credential_y
        pc3.loc[(pc3.Credential_x == " ") &
                pc3.Credential_y.notnull(), 'Credential_x'] = pc3.Credential_y
        pc3 = (pc3.drop(columns='Credential_y')
                  .drop_duplicates()
                  .rename(columns={'Credential_x': 'Credential'}))
        self.practitioner_type = (pc3.merge(
            pc[['NPI', 'Primary specialty']].drop_duplicates(), how='right'))


def medicare_program_engagement():
    """
    Produces a wide dataset at the NPI level that shows when a provider entered
    and exited the three different medicare databases: Part B, Part D, and
    Physician Compare
    """
    from .utils.globalcache import c
    partd = part_d_files(summary=True,
                         usecols=['npi', 'total_claim_count'])
    partd_engage = (partd.assign(PartD_Max_Year=lambda df: df.Year,
                                 PartD_Min_Year=lambda df: df.Year)
                         .groupby('npi', as_index=False)
                         .agg({'PartD_Min_Year': min, 'PartD_Max_Year': max})
                    )
    partb = part_b_files(summary=True,
                         columns=['National Provider Identifier',
                                  'Number of Medicare Beneficiaries'])
    partb_engage = (partb.assign(PartB_Max_Year=lambda df: df.Year,
                                 PartB_Min_Year=lambda df: df.Year)
                         .groupby('National Provider Identifier',
                                  as_index=False)
                         .agg({'PartB_Min_Year': min, 'PartB_Max_Year': max})
                         .rename(columns={'National Provider Identifier':
                                          'npi'}))
    pc = c.physician_compare_select_vars([],
                                         drop_duplicates=False,
                                         date_var=True)
    pc_engage = (pc.assign(Year=pc.date.dt.year)
                   .drop(columns='date')
                   .drop_duplicates())
    pc_engage = (pc_engage.assign(PC_Max_Year=lambda df: df.Year,
                                  PC_Min_Year=lambda df: df.Year)
                          .groupby('NPI', as_index=False)
                          .agg({'PC_Min_Year': min, 'PC_Max_Year': max})
                          .rename(columns={'NPI': 'npi'}))
    df = (pc_engage
          .merge(partd_engage, how='outer')
          .merge(partb_engage, how='outer')
          .convert_dtypes({x: 'Int64' for x in pc_engage.columns}))

    df.loc[((df.PC_Max_Year == 2020)
            | (df.PartD_Max_Year == 2017)
            | (df.PartB_Max_Year == 2017))
           & ~((df.PartD_Max_Year.notnull()
                & df.PartB_Max_Year.notnull()
                & (df.PC_Max_Year < 2020))), 'maybe_active'] = True
    df = df.assign(maybe_active=df.maybe_active.fillna(False))
    df.loc[df.PC_Max_Year == 2020, 'active_2020'] = True
    df = df.assign(active_2020=df.active_2020.fillna(False))
    return df


def medical_school(include_web_scraped=True):
    """
    Returns medical schools and graduation dates at the NPI level
    Has been unique-ified by dropping "other" entries, and then
    randomly choosing between duplicates if one isn't other
    Also has the option of bringing in approx. 127 entries from the
    web-scraped database.
    """
    from .utils.globalcache import c
    cols = ['Medical school name', 'Graduation year']
    med_school = c.physician_compare_select_vars(cols)
    nodups = med_school[~med_school['NPI'].duplicated(keep=False)]
    isid(nodups, ['NPI'], noisily=True)
    dups = med_school[med_school['NPI'].duplicated(keep=False)]
    others = dups.dropna()[dups.dropna()['Medical school name'] == "OTHER"]
    others = others.drop_duplicates(subset='NPI', keep='first')
    dups = dups.dropna()[dups.dropna()['Medical school name'] != "OTHER"]
    dups = dups.drop_duplicates(subset='NPI', keep='first')
    final = (nodups.append(dups)
                   .append(others[~others.NPI.isin(nodups.append(dups).NPI)]))
    final = (final.append(med_school[~med_school.NPI.isin(final.NPI)]
                          .assign(
                            oth=lambda x: x['Medical school name'] == "OTHER")
                          .sort_values(['NPI', 'oth'])
                          .drop_duplicates(subset='NPI', keep='first')
                          .drop(columns='oth'))
                  .rename(columns={'NPI': 'npi'}))
    if include_web_scraped:
        schools = pd.read_csv(med_school_path)
        schools = (schools[~schools.npi.isin(final.npi)]
                   .assign(notnull=lambda x: (x.medical_school_upper.notnull()
                                              & x.grad_year.notnull()))
                   .query('notnull==True')
                   .drop(columns='notnull')
                   .convert_dtypes({'npi': int,
                                    'medical_school_upper': 'string',
                                    'grad_year': 'Int64'}))
        isid(schools, 'npi')
        final = final.append(schools.rename(
            columns={'medical_school_upper': 'Medical school name',
                     'grad_year': 'Graduation year'})).reset_index(drop=True)
        final['npi'] = final.npi.astype(int)
    return final


def group_practices_infer():
    # 2. Get group practice information. most sole practitioners
    # are missing a group practice ID
    pecos_groups_loc = PECOS(['NPI', 'Organization legal name',
                              'Group Practice PAC ID',
                              'Number of Group Practice members',
                              'State', 'Zip Code', 'Phone Number'],
                             drop_duplicates=False, date_var=True)

    # Groups can change over time so start with groups
    # with same dets over time
    groups = (pecos_groups_loc
              .physician_compare
              .drop_duplicates()
              .reset_index(drop=True))

    groups.loc[groups['Organization legal name'] == " ",
               'Organization legal name'] = np.nan
    groups.loc[groups['Phone Number'].astype(str).str.len() != 12,
               'Phone Number'] = np.nan

    groups = groups.drop_duplicates()

    # Split into dfs with group ids and no group ids
    groupinfo = (groups.loc[groups['Group Practice PAC ID'].notnull()]
                       .reset_index(drop=True))
    missinggroup = (groups.loc[groups['Group Practice PAC ID'].isnull()]
                          .reset_index(drop=True))

    # Get any information that is consistent by NPI and Group Practice
    # and update the missing information
    # this mostly fixes phone numbers
    idcols = ['NPI', 'Group Practice PAC ID']
    cols = ['Zip Code', 'Organization legal name',
            'Number of Group Practice members', 'State', 'Phone Number']
    for col in cols:
        df_u = isolate_consistent_info(groupinfo, idcols, col)
        groupinfo = update_cols(groupinfo, df_u, idcols)

    groupinfo = groupinfo.drop_duplicates().reset_index(drop=True)

    # do the same fill-in for people missing group practice ids
    npis_in_both = (missinggroup[['NPI']]
                    .drop_duplicates()
                    .merge(groupinfo['NPI'].drop_duplicates()))
    mg1 = (missinggroup
           .merge(npis_in_both, how='left', indicator=True)
           .query('_merge=="left_only"')
           .drop(columns='_merge'))
    mg2 = (missinggroup
           .merge(npis_in_both, how='left', indicator=True)
           .query('_merge=="both"')
           .drop(columns='_merge'))
    idcols = ['NPI']
    cols = ['Zip Code', 'Organization legal name',
            'Number of Group Practice members', 'State', 'Phone Number']
    for col in cols:
        df_u = isolate_consistent_info(mg1, idcols, col)
        if not df_u.empty:
            mg1 = update_cols(mg1, df_u, idcols)

    missinggroup = mg1.append(mg2).drop_duplicates().reset_index(drop=True)

    # Can now look for people missing group numbers, to see if there
    # is a unique match on state, zip, phone, date
    # the majority of cases there are multiple group numbers
    # but perhaps that's not actually right... perhaps these should be joined
    pot_missing = (missinggroup[['State', 'Zip Code', 'Phone Number', 'date']]
                   .drop_duplicates()
                   .dropna()
                   .reset_index(drop=True)
                   .reset_index()
                   .merge(groupinfo[['State', 'Zip Code', 'Phone Number',
                                     'date', 'Group Practice PAC ID',
                                     'Organization legal name']]
                          .dropna(subset=['State', 'Zip Code', 'Phone Number',
                                          'date', 'Group Practice PAC ID'])
                          .drop_duplicates()))
    miss = (pot_missing[~pot_missing['index']
            .duplicated(keep=False)]
            .drop(columns='index'))
    missinggroup_u = update_cols(missinggroup, miss,
                                 ['State', 'Zip Code', 'Phone Number', 'date'])

    groupinfo1 = (missinggroup_u
                  .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
                  .reset_index(drop=True))
    missinggroup = (missinggroup_u
                    .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
                    .reset_index(drop=True))
    groupinfo = (groupinfo.append(groupinfo1)
                          .drop_duplicates().reset_index(drop=True))

    # Sole practices: number of group prac==1, npi, sometimes missing sometimes
    # one group prac
    solepracs = missinggroup[
                    missinggroup['Number of Group Practice members'] == 1
                    & missinggroup['Group Practice PAC ID'].isnull()]
    # the below dropped vars all missing
    solepracs = (solepracs
                 .drop(columns=['date', 'Group Practice PAC ID',
                                'Organization legal name', 'Phone Number'])
                 .drop_duplicates())
    solepracs = solepracs.merge(groupinfo)
    df_s = isolate_consistent_info(
        solepracs, ['NPI', 'Group Practice PAC ID'], 'Phone Number')
    solepracs = update_cols(solepracs, df_s, ['NPI', 'Group Practice PAC ID'])
    solepracs = (solepracs
                 .drop(columns=['date'])
                 .drop_duplicates()[~solepracs
                                    .drop(columns=['date'])
                                    .drop_duplicates()
                                    .NPI.duplicated(keep=False)])
    missinggroup_u = update_cols(
        missinggroup, solepracs, ['NPI', 'Number of Group Practice members'])
    groupinfo1 = (missinggroup_u
                  .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
                  .reset_index(drop=True))
    missinggroup = (missinggroup_u
                    .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
                    .reset_index(drop=True))
    groupinfo = (groupinfo.append(groupinfo1)
                          .drop_duplicates().reset_index(drop=True))

    # People exist across datasets, can match on NPI, without regard to date
    miss_unique = (missinggroup[['State', 'Zip Code', 'Phone Number', 'NPI']]
                   .drop_duplicates())
    miss_unique = miss_unique[~miss_unique.NPI.duplicated(keep=False)]
    miss_unique = miss_unique.merge(groupinfo)
    ids = ['State', 'Zip Code', 'Phone Number', 'NPI', 'Group Practice PAC ID']
    miss_unique = (miss_unique[ids].drop_duplicates()[~miss_unique[ids]
                                                      .drop_duplicates()
                                                      .NPI
                                                      .duplicated(keep=False)])
    missinggroup_u = update_cols(missinggroup,
                                 miss_unique,
                                 ['NPI', 'State', 'Zip Code', 'Phone Number'])
    groupinfo1 = (missinggroup_u
                  .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
                  .reset_index(drop=True))
    missinggroup = (missinggroup_u
                    .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
                    .reset_index(drop=True))
    groupinfo = (groupinfo.append(groupinfo1)
                          .drop_duplicates().reset_index(drop=True))

    # similar, slightly different approach
    ids = ['NPI', 'Number of Group Practice members', 'Zip Code',
           'Phone Number', 'State']
    d = missinggroup[ids].drop_duplicates().merge(
        groupinfo[ids + ['Group Practice PAC ID']].drop_duplicates())

    d = d[~d.NPI.duplicated(keep=False)]
    missinggroup_u = update_cols(missinggroup, d, ids)

    groupinfo1 = (missinggroup_u
                  .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
                  .reset_index(drop=True))
    missinggroup = (missinggroup_u
                    .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
                    .reset_index(drop=True))
    groupinfo = (groupinfo.append(groupinfo1)
                          .drop_duplicates().reset_index(drop=True))

    missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)

    # Make some new groupnos for unmatched sole proprietors
    m = missinggroup[missinggroup['Number of Group Practice members'] == 1][['NPI','Zip Code','State']].drop_duplicates().reset_index(drop=True).reset_index() 
    m['Group Practice PAC ID'] = (m['index'] + 200000000000).astype('Int64')
    missinggroup_u = update_cols(
        missinggroup, m.drop(columns='index'), ['NPI', 'State', 'Zip Code'])

    groupinfo1 = (missinggroup_u
                  .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
                  .reset_index(drop=True))
    missinggroup = (missinggroup_u
                    .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
                    .reset_index(drop=True))
    groupinfo = (groupinfo.append(groupinfo1)
                          .drop_duplicates().reset_index(drop=True))
    missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)

    m = missinggroup[['State', 'Zip Code', 'Phone Number', 'date']].drop_duplicates().merge(groupinfo[['State', 'Zip Code', 'Phone Number', 'date', 'Group Practice PAC ID']].drop_duplicates()) 

    new_groups = m[~m[['State', 'Zip Code', 'Phone Number', 'date']].duplicated(keep=False)] 
    missinggroup_u = update_cols(
        missinggroup, new_groups, ['State', 'Zip Code', 'Phone Number', 'date'])
    
    groupinfo1 = (missinggroup_u
                  .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
                  .reset_index(drop=True))
    missinggroup = (missinggroup_u
                    .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
                    .reset_index(drop=True))
    groupinfo = (groupinfo.append(groupinfo1)
                          .drop_duplicates().reset_index(drop=True))

    missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)

    # explanation here. match only on zip and phone

    k = missinggroup[['Zip Code', 'Phone Number']].drop_duplicates().merge(groupinfo, how='left', indicator=True)
    # k2 = k.query('_merge=="left_only"')
    # k2 = k2.reset_index(drop=True).reset_index()
    # k2['Group Practice PAC ID'] = (k2['index'] + 300000000000).astype('Int64')

    k1 = k.query('_merge=="both"')[['Zip Code', 'Phone Number', 'Group Practice PAC ID']].drop_duplicates()
    zip_phone_pacids = k1[~k1['Group Practice PAC ID'].duplicated(keep=False)]
    missinggroup_u = update_cols(
        missinggroup, zip_phone_pacids, ['Zip Code', 'Phone Number'])
    
    groupinfo1 = (missinggroup_u
                  .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
                  .reset_index(drop=True))
    missinggroup = (missinggroup_u
                    .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
                    .reset_index(drop=True))
    groupinfo = (groupinfo.append(groupinfo1)
                          .drop_duplicates().reset_index(drop=True))

    missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)

    # Fill in for anyone missing, where you are the same location if you
    # have the same phone number and zip at the same date

    k2 = missinggroup[['Zip Code', 'Phone Number', 'date']].dropna().drop_duplicates().reset_index(drop=True).reset_index() 
    k2['Group Practice PAC ID'] = (k2['index'] + 300000000000).astype('Int64')
    k2 = k2.drop(columns='index')
    missinggroup_u = update_cols(
        missinggroup, k2, ['Zip Code', 'Phone Number', 'date'])
    groupinfo1 = (missinggroup_u
                  .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
                  .reset_index(drop=True))
    missinggroup = (missinggroup_u
                    .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
                    .reset_index(drop=True))
    groupinfo = (groupinfo.append(groupinfo1)
                          .drop_duplicates().reset_index(drop=True))
    missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)

    isid(missinggroup, ['NPI', 'date', 'Zip Code', 'State'])

    # Finally, make up ids for nonmatches
    k3 = missinggroup.reset_index(drop=True).reset_index()
    k3['Group Practice PAC ID'] = (k3['index'] + 400000000000).astype('Int64')
    k3 = k3.drop(columns='index')

    missinggroup_u = update_cols(
        missinggroup, k3, ['NPI', 'Zip Code', 'State', 'date'])
    groupinfo1 = (missinggroup_u
                  .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
                  .reset_index(drop=True))
    missinggroup = (missinggroup_u
                    .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
                    .reset_index(drop=True))
    groupinfo = (groupinfo.append(groupinfo1)
                          .drop_duplicates().reset_index(drop=True))
    missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)
    assert missinggroup.shape[0] == 0
    return groupinfo



# This npi is only in the missing group: should at least fill in zip and telephone
# missinggroup.query('NPI=="1235197823"').sort_values(['NPI', 'date'])

# # Group by all variables before making group practice indicator

# df1 = isolate_consistent_info(groupinfo, idcols, 'zip9')
# df2 = isolate_consistent_info(groupinfo, idcols, 'zip5')
# df3 = isolate_consistent_info(groupinfo, idcols, 'Organization legal name')
# df4 = isolate_consistent_info(groupinfo, idcols,
#                               'Number of Group Practice members')
# df5 = isolate_consistent_info(groupinfo, idcols, 'State')
# df6 = isolate_consistent_info(groupinfo, idcols, 'Phone Number')

# groupinfo = update_cols(groupinfo, df1, idcols)
# groupinfo = update_cols(groupinfo, df2, idcols)
# groupinfo = update_cols(groupinfo, df3, idcols)
# groupinfo = update_cols(groupinfo, df4, idcols)
# groupinfo = update_cols(groupinfo, df5, idcols)
# groupinfo = update_cols(groupinfo, df6, idcols)

# df6['Phone Number'] = df6['Phone Number'].astype(float).astype(int)

# groups = groups.reset_index(drop=True).reset_index()
# # A bunch of sole practitioners (groupsize =1 ) are missing
# # give them a single-period group practice ID (not constant over
# # time even though other IDs are)
# groups.loc[
#     groups['Group Practice PAC ID'].isnull(),
#     'Group Practice PAC ID'] = (groups['index'] + 100000000000)
# groups = groups.drop(columns='index')
# groups = groups.merge(
#     groups[['NPI', 'Group Practice PAC ID', 'date']]
#     .drop_duplicates()
#     .groupby(['Group Practice PAC ID', 'date'])
#     .size()
#     .reset_index())
# groups.loc[
#     groups['Number of Group Practice members'].isnull(),
#     'Number of Group Practice members'] = groups[0]
# groups.drop(columns=[0], inplace=True)


def match_npi_to_groups(groupinfo):
    npi = NPI(entities=1)
    npi.retrieve('ploctel')
    npi.retrieve('ploczip')
    npi.retrieve('plocstatename')
    npi.retrieve('practitioner_type')

    s = npi.practitioner_type.set_index('npi')[['MD/DO', 'NP']].sum(axis=1) > 0
    mds_nps = s[s].reset_index().drop(columns=0)
    practypes = npi.practitioner_type.merge(mds_nps)[['npi', 'MD/DO', 'NP']]
    locdata = npi.plocstatename.merge(mds_nps).merge(npi.ploczip.merge(mds_nps)).merge(npi.ploctel.merge(mds_nps)) 
    locdata = locdata.assign(quarter=pd.PeriodIndex(locdata.month, freq='Q'))
    locdata = locdata.drop(columns='month')

    match_groups = groupinfo.assign(quarter=pd.PeriodIndex(groupinfo.date, freq='Q'))[['Group Practice PAC ID', 'quarter', 'State', 'Phone Number', 'Zip Code']].drop_duplicates()
    match_groups = match_groups.reset_index(drop=True)

    match_groups_nophone = match_groups.loc[match_groups['Phone Number'].isnull()].drop(columns='Phone Number')
    match_groups_phone = match_groups.loc[match_groups['Phone Number'].notnull()]

    match_groups_phone = match_groups_phone[~match_groups_phone[['quarter', 'State',  'Phone Number', 'Zip Code']].duplicated(keep=False)]
    match_groups_nophone = match_groups_nophone[~match_groups_nophone[['quarter', 'State', 'Zip Code']].duplicated(keep=False)]


def isolate_consistent_info(df, idcols, targetcol):
    """
    If a column is consistent among the id variables,
    this retrieves that info, unique at the idcols
    level. we assume this is the master information
    """
    df = df[idcols + [targetcol]].drop_duplicates().dropna()
    df = df[~df[idcols].duplicated(keep=False)]
    isid(df, idcols)
    return df


def update_cols(df, update_df, idcols):
    """
    Fills in any missing details of a column with new information,
    merged in on the idcols level
    """
    df = df.merge(update_df, on=idcols, how='left')
    update_cols = [x for x in update_df.columns if x not in idcols]
    for col in update_cols:
        df[col] = df[f'{col}_x'].fillna(df[f'{col}_y'])
        df = df.drop(columns=[f'{col}_x', f'{col}_y'])
    return df
