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

    def fix_zips(self):
        from .utils.globalcache import c
        pecos = self.physician_compare[['Zip Code', 'State',
                                        'Group Practice PAC ID']]
        self.physician_compare = self.physician_compare.assign(
            **{'Zip Code': c.fix_pecos_zips(pecos)})


def fix_pecos_zips(pecos):
    # Fix misshapen zip codes - 8s and 4s in states that have 0 prefixes
    zip0_states = ['CT', 'MA', 'ME', 'NH', 'NJ', 'PR', 'RI', 'VT', 'VI']
    pecos = pecos.assign(**{'Zip Code': np.where(
        ((pecos['Zip Code'].str.len() == 8) |
            (pecos['Zip Code'].str.len() == 4))
        & (pecos.State.isin(zip0_states)),
        '0' + pecos['Zip Code'], pecos['Zip Code'])})

    # Fix misshapen zip codes - double zeros
    busted_zips = (pecos[
        (pecos['Zip Code'].str.len() != 9)
        & (pecos['Zip Code'].str.len() != 5)]
        ['Group Practice PAC ID'].drop_duplicates().dropna())
    z = (pecos
         .merge(busted_zips)
         .groupby(['Group Practice PAC ID', 'Zip Code'])
         .size())
    zip_crosswalk = (z.reset_index()
                     [z.reset_index()['Zip Code'].str.len() == 3]
                     .drop(columns=0)
                     .assign(zip2=lambda df: '00' + df['Zip Code'])
                     .merge(z.reset_index().drop(columns=0),
                            left_on=['Group Practice PAC ID', 'zip2'],
                            right_on=['Group Practice PAC ID', 'Zip Code'])
                     .append(z.reset_index()
                             [z.reset_index()['Zip Code'].str.len() == 7]
                             .drop(columns=0)
                             .assign(zip2=lambda df: '00' + df['Zip Code'])
                             .merge(z.reset_index().drop(columns=0),
                                    left_on=['Group Practice PAC ID', 'zip2'],
                                    right_on=['Group Practice PAC ID',
                                              'Zip Code']))
                     .drop(columns='zip2')
                     .rename(columns={'Zip Code_x': 'Zip Code'})
                     .assign(**{
                        'Group Practice PAC ID':
                        lambda df: df['Group Practice PAC ID'].astype('Int64')}
                        ))
    pecos = (pecos.merge(zip_crosswalk, how='left', indicator=True))
    pecos.loc[(
        pecos._merge == "both"),
        'Zip Code'] = pecos['Zip Code_y']

    pecos = (pecos.drop(columns=['_merge', 'Zip Code_y']))
    return pecos['Zip Code']




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
    pecos_groups_loc.fix_zips()
    # FIX ZIPs

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
    m = missinggroup[missinggroup['Number of Group Practice members'] == 1]
    m = (m[['NPI', 'Zip Code', 'State']]
         .drop_duplicates()
         .reset_index(drop=True)
         .reset_index())
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

    ids = ['State', 'Zip Code', 'Phone Number', 'date']
    m = (missinggroup[ids]
         .drop_duplicates()
         .merge(groupinfo[ids + ['Group Practice PAC ID']].drop_duplicates()))

    new_groups = m[~m[ids].duplicated(keep=False)]
    missinggroup_u = update_cols(
        missinggroup, new_groups, ids)

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

    k = (missinggroup[['Zip Code', 'Phone Number']]
         .drop_duplicates()
         .merge(groupinfo, how='left', indicator=True))
    # k2 = k.query('_merge=="left_only"')
    # k2 = k2.reset_index(drop=True).reset_index()
    # k2['Group Practice PAC ID'] = (
    #   k2['index'] + 300000000000).astype('Int64'))

    ids = ['Zip Code', 'Phone Number']
    k1 = (k.query('_merge=="both"')[ids + ['Group Practice PAC ID']]
           .drop_duplicates())
    zip_phone_pacids = k1[~k1['Group Practice PAC ID'].duplicated(keep=False)]
    missinggroup_u = update_cols(
        missinggroup, zip_phone_pacids, ids)

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

    k2 = (missinggroup[['Zip Code', 'Phone Number', 'date']]
          .dropna()
          .drop_duplicates()
          .reset_index(drop=True).reset_index())
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


def make_master_enrollee_dataframe(enrollee_subset,
                                   start='2013-01-01', end='2020-01-01'):
    npi = NPI(entities=1)
    npi.retrieve('ploctel')
    npi.retrieve('ploczip')
    npi.retrieve('plocstatename')

    # npi.ploctel['ploctel'] = (npi
    #                           .ploctel
    #                           .ploctel
    #                           .astype('str')
    #                           .str.split('.', expand=True)[0])
    # npi.ploctel['ploctel'] = (npi.ploctel
    #                              .ploctel
    #                              .str.replace('-', '')
    #                              .str.replace('(', '')
    #                              .str.replace(')', '')
    #                              .str.replace(' ', ''))
    # Get locations at the enrollee-quarter level. Can be duplicates at the
    # enrollee quarter. Also, drop prior to 2013 and after 2019
    locdata = (npi.plocstatename
               .merge(enrollee_subset)
               .merge(npi.ploczip.merge(enrollee_subset))
               .merge(npi.ploctel.merge(enrollee_subset)))
    locdata = locdata.assign(quarter=pd.PeriodIndex(locdata.month, freq='Q'))
    locdata = locdata.loc[locdata.quarter
                          >= pd.to_datetime(start).to_period(freq='Q')]
    locdata = locdata.loc[locdata.quarter
                          < pd.to_datetime(end).to_period(freq='Q')]
    locdata = locdata.drop(columns='month')
    locdata = locdata.drop_duplicates()
    return locdata


def get_mds_nps_info():
    npi = NPI(entities=1)
    npi.retrieve('practitioner_type')
    # get about 1.5 million MDs and NPs
    s = npi.practitioner_type.set_index('npi')[['MD/DO', 'NP']].sum(axis=1) > 0
    mds_nps = s[s].reset_index().drop(columns=0)
    practypes = npi.practitioner_type.merge(mds_nps)[['npi', 'MD/DO', 'NP']]
    return mds_nps, practypes


def make_master_group_practice_dataframe(groupinfo,
                                         start='2013-01-01', end='2020-01-01'):

    match_groups_NPI = groupinfo.assign(
        quarter=pd.PeriodIndex(groupinfo.date, freq='Q'))
    match_groups_NPI = match_groups_NPI[['NPI', 'Group Practice PAC ID',
                                         'quarter', 'State', 'Phone Number',
                                         'Zip Code']].drop_duplicates()
    match_groups_NPI = (match_groups_NPI
                        .rename(columns={'NPI': 'npi'}).reset_index(drop=True))

    # want to append the group practices from nearby quarters, to make
    #  a full panel for matching at the npi-quarter level
    group_practices_npi = (match_groups_NPI[['npi', 'quarter',
                                             'Group Practice PAC ID',
                                             'State', 'Phone Number',
                                             'Zip Code']]
                           .drop_duplicates())

    timeperiods = [('2013-04-01', '2013-01-01'),
                   ('2015-04-01', '2015-01-01'),
                   ('2016-04-01', '2016-01-01'),
                   ('2017-04-01', '2017-01-01'),
                   ('2019-10-01', '2019-07-01')]

    addlist = [(group_practices_npi
                .loc[group_practices_npi.quarter
                     == pd.to_datetime(t[0]).to_period(freq='Q')]
                .assign(quarter=pd.to_datetime(t[1]).to_period(freq='Q')))
               for t in timeperiods]

    group_practices_npi = pd.concat([group_practices_npi] + addlist)
    group_practices_npi = group_practices_npi.loc[group_practices_npi.quarter
                                                  >= (pd
                                                      .to_datetime(start)
                                                      .to_period(freq='Q'))]
    group_practices_npi = group_practices_npi.loc[group_practices_npi.quarter
                                                  < (pd
                                                     .to_datetime(end)
                                                     .to_period(freq='Q'))]
    group_practices_npi = group_practices_npi.reset_index(drop=True)
    # Why is this necessary
    group_practices_npi = group_practices_npi.assign(
        quarter=group_practices_npi.quarter.astype('period[Q-DEC]'))
    return group_practices_npi


def match_npi_to_groups(locdata, group_practices_npi):

    # Merge the NPPES data to the group practice data, merging on npi-quarter
    already_have_group = locdata.merge(group_practices_npi)
    # Note: this converts missing telephones to string "nan"
    already_have_group = (already_have_group[['npi', 'quarter',
                                              'plocstatename', 'ploctel',
                                              'ploczip',
                                              'Group Practice PAC ID']]
                          .rename(columns={'plocstatename': 'state',
                                           'ploctel': 'telephone',
                                           'ploczip': 'zip'})
                          .assign(telephone=lambda x: (x['telephone']
                                                       .astype(str)
                                                       .str.split('.')
                                                       .str[0]))
                          .append(already_have_group[['npi', 'quarter',
                                                      'State', 'Phone Number',
                                                      'Zip Code',
                                                      'Group Practice PAC ID']]
                                  .rename(columns={'State': 'state',
                                                   'Phone Number': 'telephone',
                                                   'Zip Code': 'zip'})
                                  .assign(telephone=lambda x: (x['telephone']
                                                               .astype(str)
                                                               .str
                                                               .split('.')
                                                               .str[0]))
                                  )
                          .drop_duplicates())

    npi_location_nums = (locdata.groupby(['plocstatename', 'ploczip',
                                          'ploctel', 'quarter'])
                                .size()
                                .reset_index()
                                .reset_index()
                                .assign(npi_group_id=lambda df: df['index']
                                        + 900000000000))
    groups_combined = (npi_location_nums
                       .drop(columns='index')
                       .rename(columns={'plocstatename': 'state',
                                        'ploctel': 'telephone',
                                        'ploczip': 'zip'})
                       .merge(already_have_group, how='left', indicator=True))
    groups_combined.loc[groups_combined._merge == 'left_only',
                        'group_id'] = groups_combined.npi_group_id
    groups_combined.loc[groups_combined._merge == 'both',
                        'group_id'] = groups_combined['Group Practice PAC ID']
    groups_combined['group_id'] = groups_combined.group_id.astype(int)

    missing_group = (locdata.merge(already_have_group[['npi', 'quarter']]
                     .drop_duplicates(), how='left', indicator=True)
                     .query('_merge=="left_only"')
                     .drop(columns='_merge')
                     .assign(telephone=lambda x:
                             (x['ploctel'].astype(str).str.split('.').str[0]))
                     .drop(columns='ploctel'))

    # This will add groups for everyone but a small number of people missing
    # zip, state, or tel in the
    # NPPES
    add_groups = (missing_group
                  .rename(columns={'plocstatename': 'state',
                                   'ploczip': 'zip'})
                  .merge(groups_combined
                         .drop(columns=['npi', 0, 'npi_group_id',
                                        'Group Practice PAC ID', '_merge'])
                         .drop_duplicates()))

    full_groups = (already_have_group
                   .rename(columns={'Group Practice PAC ID': 'group_id'})
                   .append(add_groups)
                   .drop_duplicates())
    full_groups['group_quarter_loc_id'] = (full_groups['group_id'].astype(str)
                                           + '-'
                                           + full_groups['quarter'].astype(str)
                                           + '-'
                                           + full_groups['state']
                                           + '-'
                                           + full_groups['zip'].astype(str)
                                           + '-'
                                           + full_groups['telephone'])
    return full_groups


def count_nps_for_mds_q(full_groups_ids_q):
    return (full_groups_ids_q
            .loc[full_groups_ids_q['MD/DO'] == 1]
            .drop(columns=['MD/DO', 'NP'])
            .merge(full_groups_ids_q.loc[full_groups_ids_q['NP'] == 1],
                   on=['quarter', 'group_quarter_loc_id'])
            [['npi_x', 'npi_y', 'quarter']]
            .drop_duplicates()
            .groupby(['npi_x', 'quarter']).size())


def count_nps_for_mds(locdata, full_groups_ids, practypes):
    from .utils.globalcache import c
    quarters = sorted(locdata.quarter.value_counts().index)
    npc = pd.concat(
        [c.count_nps_for_mds_q(full_groups_ids[full_groups_ids.quarter == q])
         for q in quarters])
    npc_all = (locdata
               .merge(practypes.loc[practypes['MD/DO'] == 1])
               .merge(npc.reset_index().rename(
                columns={'npi_x': 'npi', 0: 'npcount'})))
    npc_all = (locdata.merge(practypes.loc[practypes['MD/DO'] == 1])
               .merge(npc
                      .reset_index()
                      .rename(columns={'npi_x': 'npi', 0: 'npcount'}),
                      how='left')
               .drop(columns=['MD/DO', 'NP'])
               .fillna(0)
               .sort_values(['npi', 'quarter'])
               .reset_index(drop=True)
               .assign(npcount=lambda df: df.npcount.astype(int)))
    return npc_all


def count_nps_for_mds_master():
    from .utils.globalcache import c
    mds_nps, practypes = c.get_mds_nps_info()
    locdata = c.make_master_enrollee_dataframe(mds_nps)
    groupinfo = c.group_practices_infer()
    group_practices_npi = c.make_master_group_practice_dataframe(groupinfo)
    full_groups = c.match_npi_to_groups(locdata, group_practices_npi)
    full_groups_ids = (full_groups[['npi', 'quarter', 'group_quarter_loc_id']]
                       .drop_duplicates()
                       .merge(practypes))
    npcs = c.count_nps_for_mds(
        locdata[['npi', 'quarter']].drop_duplicates(),
        full_groups_ids, practypes)
    return npcs

    # from project_management.helper import hash_retrieve
    # groupinfo = hash_retrieve('2254943319023394300')


    # locdata_phone = locdata.loc[locdata.ploctel.notnull()]
    # locdata_nophone = locdata.loc[locdata.ploctel.isnull()]

    # Groupinfo is from the laborious process to assign practice groups, above
    # get state, phone, zip for each group-quarter
    # Group ids can stay constant across quarters but don't necessarily
#    match_groups = groupinfo.assign(
#        quarter=pd.PeriodIndex(groupinfo.date, freq='Q'))
#    match_groups = match_groups[['Group Practice PAC ID', 'quarter', 'State',
#                                 'Phone Number', 'Zip Code']].drop_duplicates()
#    match_groups = match_groups.reset_index(drop=True)
#
#    match_groups_nophone = (match_groups
#                            .loc[match_groups['Phone Number'].isnull()]
#                            .drop(columns='Phone Number'))
#    match_groups_phone = (match_groups
#                          .loc[match_groups['Phone Number'].notnull()])
#
#    id1 = ['quarter', 'State',  'Phone Number', 'Zip Code']
#    id2 = ['quarter', 'State', 'Zip Code']
#    match_groups_phone = match_groups_phone[~match_groups_phone[id1]
#                                            .duplicated(keep=False)]
#    match_groups_nophone = match_groups_nophone[~match_groups_nophone[id2]
#                                                .duplicated(keep=False)]

    # match_groups_npi keeps the group information at the npi - quarter level

    # prelim_matches = (missing_group.rename(columns={'plocstatename': 'state',
    #                                                 'ploctel': 'telephone',
    #                                                 'ploczip': 'zip'})
    #                                .assign(telephone=lambda x: (x['telephone']
    #                                                             .astype(str)
    #                                                             .str.split('.')
    #                                                             .str[0]))
    #                                .merge(already_have_group,
    #                                       on=[x for x in
    #                                           already_have_group.columns
    #                                           if x not in
    #                                           ['npi',
    #                                            'Group Practice PAC ID']],
    #                                       how='left', indicator=True))
# 
# 
# 
    # missing_group.merge(prelim_matches[['npi_x','quarter']].rename(columns={'npi_x': 'npi'}).drop_duplicates(), how='left', indicator=True) 
# 
    # match_groups_phone.assign(ploctel=match_groups_phone['Phone Number'].astype(str).str.split('.').str[0], ploczip = match_groups_phone['Zip Code'], plocstatename=match_groups_phone['State'])
    # m.query('_merge=="left_only"').drop(columns='Group Practice PAC ID').merge(match_groups_phone.assign(ploctel=match_groups_phone['Phone Number'].astype(str).str.split('.').str[0], ploczip = match_groups_phone['Zip Code'], plocstatename=match_groups_phone['State']))



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
