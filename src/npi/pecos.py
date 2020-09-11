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
            **{'Zip Code': c.fix_pecos_zips(pecos).astype('string')
               })

    def fix_phones(self):
        from .utils.globalcache import c
        pecos = self.physician_compare[['NPI', 'date', 'Group Practice PAC ID',
                                        'State', 'Zip Code', 'Phone Number']]
        phones = c.fix_pecos_phones(pecos)
        self.physician_compare = (self
                                  .physician_compare
                                  .reset_index()
                                  .drop(columns='Phone Number')
                                  .merge(phones.reset_index())
                                  .drop(columns='index'))

    def fix_orgs(self):
        self.physician_compare.loc[
            self.physician_compare['Organization legal name'] == " ",
            'Organization legal name'] = np.nan


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


def fix_pecos_phones(df):
    df = df.assign(**{'Phone Number': df['Phone Number'].astype(str)})
    df = df.assign(**{'Phone Number': np.where(df['Phone Number'] == '<NA>',
                      'nan', df['Phone Number'])})
    df = df.assign(
        **{'Phone Number': df['Phone Number'].str.split('.', expand=True)[0]})
    print('Unusual entries for phone number of lengths 5 and 6:')
    print(df[(df['Phone Number'].str.len() < 10)
             & (df['Phone Number'] != 'nan')]['Phone Number']
          .str.len().value_counts())
    df.loc[(df['Phone Number'].str.len() < 10) & (df['Phone Number'] != 'nan'),
           'Phone Number'] = 'nan'
    # If the same NPI-practice-state-zip has only one phone number and nan,
    # replace with the phone number
    phones = df[['NPI', 'Group Practice PAC ID',
                 'State', 'Zip Code', 'Phone Number']].drop_duplicates()
    phones.reset_index(drop=True, inplace=True)
    phones_replace = (phones
                      .loc[phones['Phone Number'] == 'nan']
                      .merge(phones.loc[phones['Phone Number'] != 'nan'],
                             on=['NPI', 'Group Practice PAC ID',
                                 'State', 'Zip Code']))
    phones_replace = (phones_replace[~phones_replace[['NPI',
                                                      'Group Practice PAC ID',
                                                      'State', 'Zip Code']]
                                     .duplicated(keep=False)]
                      .drop(columns='Phone Number_x')
                      .rename(columns={'Phone Number_y': 'Phone Number'}))
    df = df.merge(phones_replace,
                  on=['NPI', 'Group Practice PAC ID', 'State', 'Zip Code'],
                  how='left')
    df.loc[(df['Phone Number_x'] == 'nan')
           & (df['Phone Number_y'].notnull()),
           'Phone Number_x'] = df['Phone Number_y']
    df = (df
          .drop(columns='Phone Number_y')
          .rename(columns={'Phone Number_x': 'Phone Number'}))
    # If the same date-practice-state-zip has only one phone number and nan,
    # replace with the phone number
    phones = df[['date', 'Group Practice PAC ID',
                 'State', 'Zip Code', 'Phone Number']].drop_duplicates()
    phones.reset_index(drop=True, inplace=True)
    phones_replace = (phones
                      .loc[phones['Phone Number'] == 'nan']
                      .merge(phones.loc[phones['Phone Number'] != 'nan'],
                             on=['date', 'Group Practice PAC ID',
                                 'State', 'Zip Code']))
    phones_replace = (phones_replace[~phones_replace[['date',
                                                      'Group Practice PAC ID',
                                                      'State', 'Zip Code']]
                                     .duplicated(keep=False)]
                      .drop(columns='Phone Number_x')
                      .rename(columns={'Phone Number_y': 'Phone Number'}))
    df = df.merge(phones_replace,
                  on=['date', 'Group Practice PAC ID', 'State', 'Zip Code'],
                  how='left')
    df.loc[(df['Phone Number_x'] == 'nan')
           & (df['Phone Number_y'].notnull()),
           'Phone Number_x'] = df['Phone Number_y']
    df = (df
          .drop(columns='Phone Number_y')
          .rename(columns={'Phone Number_x': 'Phone Number'}))
    assert not [x for x in df['Phone Number'].values if not isinstance(x, str)]
    # df = df.assign(**{'Phone Number': df['Phone Number'].astype('string')})
    return df


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


def group_practices_info_infer():
    """
    This is light-touch inferring, actually doesn't do a great job, only
    reduces missing groups by 11% to 10%
    """
    # 1. Read in physician compare data
    pecos_groups_loc = PECOS(['NPI', 'Organization legal name',
                              'Group Practice PAC ID',
                              'Number of Group Practice members',
                              'State', 'Zip Code', 'Phone Number'],
                             drop_duplicates=False, date_var=True)
    pecos_groups_loc.fix_zips()
    pecos_groups_loc.fix_phones()
    pecos_groups_loc.fix_orgs()

    groups = (pecos_groups_loc
              .physician_compare
              .drop_duplicates()
              .reset_index(drop=True))

    ###########################################################################
    # 2. Split into dfs with group ids and no group ids
    groupinfo = (groups.loc[groups['Group Practice PAC ID'].notnull()]
                       .reset_index(drop=True))
    missinggroup = (groups.loc[groups['Group Practice PAC ID'].isnull()]
                          .reset_index(drop=True))

    # want NPIs that are in the groupinfo data to always
    # be included in groupinfo
    npis_in_both = (missinggroup[['NPI']]
                    .drop_duplicates()
                    .merge(groupinfo['NPI'].drop_duplicates()))

    # people missing group practice IDs who have them at some point and are
    # in groupinfo - add to groupinfo to impute

    groupinfo = groupinfo.append(missinggroup.merge(npis_in_both))

    # people missing group practice IDs who never have them at any point
    missinggroup = (missinggroup
                    .merge(npis_in_both, how='left', indicator=True)
                    .query('_merge=="left_only"')
                    .drop(columns='_merge'))

    ###########################################################################

    # if only one group practice ID for an NPI-state-zip code-Phone; fill in
    idcols = ['NPI', 'State', 'Zip Code', 'Phone Number']
    updatecols = ['Group Practice PAC ID']
    groupinfo = batch_update_cols_with_consistent_info(
        groupinfo, idcols, updatecols)

    # fill in missing information at the NPI-Group Practice ID if there is only
    # one listed, e.g., phone number for that NPI-Practice ID
    idcols = ['NPI', 'Group Practice PAC ID']
    updatecols = ['Zip Code', 'Organization legal name',
                  'Number of Group Practice members', 'State', 'Phone Number']
    groupinfo = batch_update_cols_with_consistent_info(
        groupinfo, idcols, updatecols)

    ###########################################################################
    early_years = groupinfo[~groupinfo.date.dt.year.isin(range(2018, 2021))]
    late_years = groupinfo[groupinfo.date.dt.year.isin(range(2018, 2021))]

    idcols = ['NPI', 'Group Practice PAC ID']
    updatecols = ['Zip Code', 'Organization legal name',
                  'Number of Group Practice members', 'State', 'Phone Number']
    early_years = batch_update_cols_with_consistent_info(
        early_years, idcols, updatecols)
    late_years = batch_update_cols_with_consistent_info(
        late_years, idcols, updatecols)

    idcols = ['NPI', 'State', 'Zip Code', 'Phone Number']
    updatecols = ['Group Practice PAC ID']
    early_years = batch_update_cols_with_consistent_info(
        early_years, idcols, updatecols)
    late_years = batch_update_cols_with_consistent_info(
        late_years, idcols, updatecols)

    ###########################################################################
    # If there is a line that contains any information on targetcols,
    # and that information is unique on NPI-State-Zip, will be used to update
    # blank lines

    idcols = ['NPI', 'State', 'Zip Code']
    targetcols = ['Organization legal name',
                  'Phone Number',
                  'Group Practice PAC ID']
    df_u = isolate_consistent_info(early_years, idcols, targetcols)
    early_years = update_cols(early_years, df_u, idcols)

    df_u = isolate_consistent_info(late_years, idcols, targetcols)
    late_years = update_cols(late_years, df_u, idcols)

    groupinfo = early_years.append(late_years).drop_duplicates()

    ###########################################################################

    # date based connections-- only if match on phone, zip, and state (no nan)

    idcols = ['date', 'State', 'Zip Code', 'Phone Number']
    updatecols = ['Group Practice PAC ID']
    groupinfo1 = groupinfo.loc[groupinfo['Phone Number'] != "nan"]
    groupinfo2 = groupinfo.loc[groupinfo['Phone Number'] == "nan"]
    groupinfo1 = batch_update_cols_with_consistent_info(
        groupinfo1, idcols, updatecols)

    groupinfo = groupinfo1.append(groupinfo2).drop_duplicates()

    ###########################################################################

    # If the state-zip-phone-date (no nan) are the same, and those are unique
    # to a group id, then infer they are the same group

    a = groupinfo.loc[
        lambda df: (df['Group Practice PAC ID'].notnull())
        & (df['Phone Number'] != 'nan')][['State', 'Zip Code', 'date',
                                          'Phone Number',
                                          'Group Practice PAC ID']]
    b = isolate_consistent_info(a,
                                ['State', 'Zip Code', 'date', 'Phone Number'],
                                'Group Practice PAC ID')
    d = update_cols(
        missinggroup.append(
            groupinfo[groupinfo['Group Practice PAC ID'].isnull()]),
        b, ['State', 'Zip Code', 'date', 'Phone Number'])

    # finally, split back out missinggroup and groupinfo
    missinggroup = d.loc[d['Group Practice PAC ID'].isnull()]
    groupinfo = groupinfo[groupinfo['Group Practice PAC ID'].notnull()].append(
        d.loc[d['Group Practice PAC ID'].notnull()])

    assert missinggroup['Group Practice PAC ID'].notnull().sum() == 0
    assert groupinfo['Group Practice PAC ID'].isnull().sum() == 0

    return groupinfo, missinggroup


def group_practices_impute(groupinfo, missinggroup):
    phones = groupinfo.append(
        missinggroup).loc[lambda df: df['Phone Number'] != 'nan']
    nophones = groupinfo.append(
        missinggroup).loc[lambda df: df['Phone Number'] == 'nan']
    phones_groupids = (phones[['State', 'Zip Code', 'Phone Number']]
                       .drop_duplicates()
                       .reset_index(drop=True)
                       .reset_index()
                       .assign(my_group_id=lambda df:
                               df['index'] + 100000000000)
                       .drop(columns='index'))
    nophones_groupids = (nophones[['NPI', 'State', 'Zip Code']]
                         .drop_duplicates()
                         .reset_index(drop=True)
                         .reset_index()
                         .assign(my_group_id=lambda df:
                                 df['index'] + 200000000000)
                         .drop(columns='index'))
    group_inferred = (phones
                      .merge(phones_groupids)
                      .append(nophones.merge(nophones_groupids)))
    group_count1 = (group_inferred[['NPI', 'Group Practice PAC ID', 'date']]
                    .drop_duplicates()
                    .groupby(['Group Practice PAC ID', 'date'])
                    .size()
                    .rename('num_group_members_GPPID_recalc')
                    .reset_index())
    group_count2 = (group_inferred[['NPI', 'my_group_id', 'date']]
                    .drop_duplicates()
                    .groupby(['my_group_id', 'date'])
                    .size()
                    .rename('num_group_members_myid_recalc')
                    .reset_index())
    return group_inferred, group_count1, group_count2


def infer_all_group_practices(group_inferred, locdata):
    """
    finalizes the group_inferred data from previously, and combines
    with the NPI-NPPES address data

    DOES include NPI nurses (who don't have a PECOS ID) into the PECOS
    group identifiers. Don't need to only use my_group_id_npi to get that
    information

    Note, this weirdly leaves in all of PECOS (including chiropractors etc)
    whereas in locdata, the NPI only has mds_nps and not other practitioners
    Shoulndn't matter though.

    all_dates will have a mixture of types, and will thus be weird for non-mds
    and nps
    """

    # make NPI-NPPES indicator of group ID
    locdata = locdata.assign(quarter=lambda df: df.quarter.dt.to_timestamp())

    group_inferred_npi = (locdata
                          .drop(columns='npi')
                          .drop_duplicates()
                          .reset_index(drop=True)
                          .reset_index()
                          .assign(my_group_id_npi=lambda df:
                                  df['index'] + 300000000000)
                          .drop(columns='index'))
    group_inferred_npi = locdata.merge(group_inferred_npi)

    group_count3 = (group_inferred_npi[['npi', 'my_group_id_npi', 'quarter']]
                    .drop_duplicates()
                    .groupby(['my_group_id_npi', 'quarter'])
                    .size()
                    .rename('num_group_members_myid_npi_recalc')
                    .reset_index())

    # Move group_inferred to quarters, and add in the missing quarters
    group_inferred_q = (group_inferred
                        .assign(quarter=lambda df:
                                pd.PeriodIndex(df.date, freq='Q'))
                        .assign(quarter=lambda df:
                                df.quarter.dt.to_timestamp())
                        [['State', 'Zip Code',
                          'quarter', 'Phone Number',
                          'Group Practice PAC ID', 'my_group_id', 'NPI']]
                        .drop_duplicates())

    timeperiods = [('2013-04-01', '2013-01-01'),
                   ('2015-04-01', '2015-01-01'),
                   ('2016-04-01', '2016-01-01'),
                   ('2017-04-01', '2017-01-01'),
                   ('2019-10-01', '2019-07-01')]

    addlist = [group_inferred_q.loc[
               group_inferred_q.quarter == pd.to_datetime(t[0])].assign(
                quarter=pd.to_datetime(t[1])) for t in timeperiods]

    group_inferred_q = pd.concat([group_inferred_q] + addlist)

    # Grab group info at the NPI-location detail level and just the
    # location-detail level. This will
    # be searched for unique group ids, then merged onto the NPI
    # this is so if, say, a nurse is listed at the same address as a group
    # practice in the CMS data
    # we can pick up on it
    # do this for both the propery group id and my one based on unique
    # phone-zip-state
    # Expands the group_inferred_q dataset by about 8 percent
    group_inferred1 = group_inferred_q.loc[
                        lambda df: df['Phone Number'] != 'nan'][
                        ['State', 'Zip Code', 'quarter', 'Phone Number',
                         'Group Practice PAC ID',
                         'my_group_id', 'NPI']].drop_duplicates()
    group_inferred2 = group_inferred_q.loc[
                        lambda df: df['Phone Number'] != 'nan'][
                        ['State', 'Zip Code', 'quarter',
                         'Phone Number', 'Group Practice PAC ID',
                         'my_group_id']].drop_duplicates()

    groups_for_npis = isolate_consistent_info(
        group_inferred1,
        ['State', 'Zip Code', 'quarter', 'Phone Number', 'NPI'],
        'Group Practice PAC ID')
    other_groups = isolate_consistent_info(
        group_inferred2,
        ['State', 'Zip Code', 'quarter', 'Phone Number'],
        'Group Practice PAC ID')

    groups_for_npis_my = isolate_consistent_info(
        group_inferred1,
        ['State', 'Zip Code', 'quarter', 'Phone Number', 'NPI'],
        'my_group_id')
    other_groups_my = isolate_consistent_info(
        group_inferred2,
        ['State', 'Zip Code', 'quarter', 'Phone Number'],
        'my_group_id')

    m1 = locdata.merge(
        groups_for_npis,
        left_on=locdata.columns.tolist(),
        right_on=['NPI', 'State', 'Zip Code', 'Phone Number', 'quarter'])
    m2 = locdata.merge(
        other_groups,
        left_on=['plocstatename', 'ploczip', 'ploctel', 'quarter'],
        right_on=['State', 'Zip Code', 'Phone Number', 'quarter'])
    m3 = locdata.merge(
        groups_for_npis_my,
        left_on=locdata.columns.tolist(),
        right_on=['NPI', 'State', 'Zip Code', 'Phone Number', 'quarter'])
    m4 = locdata.merge(
        other_groups_my,
        left_on=['plocstatename', 'ploczip', 'ploctel', 'quarter'],
        right_on=['State', 'Zip Code', 'Phone Number', 'quarter'])

    # append to the inferred groups, since this is more information on the
    # group id level for the already calculated variables
    group_inferred_q_all = (
        group_inferred_q
        .append(m2
                .append(m1.drop(columns='NPI'))
                .merge(m4.append(m3.drop(columns='NPI'))
                         .assign(my_group_id=lambda df:
                                 df.my_group_id.astype('Int64')),
                       how='outer')
                .rename(columns={'npi': 'NPI'})
                [[x for x in group_inferred_q.columns]]))
    group_inferred_q_all = (group_inferred_q_all
                            .drop_duplicates()
                            .assign(my_group_id=lambda df:
                                    df.my_group_id.astype('Int64')))

    # finally, add in group_inferred_npi, which uses only the NPPES to infer
    # groups, as calculated above
    # This expands by another 35 percent
    group_inferred_qall2 = (group_inferred_q_all
                            .merge(group_inferred_npi
                                   .rename(columns=dict(
                                    npi='NPI',
                                    plocstatename='State',
                                    ploczip="Zip Code",
                                    ploctel="Phone Number"))
                                   .assign(my_group_id_npi=lambda df:
                                           df.my_group_id_npi.astype('Int64')),
                                   how='outer'))
    group_inferred_qall2 = (group_inferred_qall2
                            .assign(NPI=lambda df: df.NPI.astype(int)))

    # Get pecos dates of involvement
    pecos_dates = (group_inferred_q[['NPI', 'quarter']]
                   .groupby('NPI')
                   .min()
                   .merge(group_inferred_q[['NPI', 'quarter']]
                          .groupby('NPI').max(),
                          left_index=True, right_index=True))
    all_dates = (group_inferred_qall2[['NPI', 'quarter']]
                 .groupby('NPI')
                 .min()
                 .merge(group_inferred_qall2[['NPI', 'quarter']]
                        .groupby('NPI').max(),
                        left_index=True, right_index=True))
    return group_inferred_qall2, group_count3, pecos_dates, all_dates


def get_md_np_counts(group_inferred_q_all, practypes):
    def get_md_np_counts_single(group_inferred_q_all, practypes, groupvar):
        return (group_inferred_q_all[['NPI', 'quarter', groupvar]]
                .loc[lambda df: df[groupvar].notnull()]
                .drop_duplicates()
                .merge(practypes.rename(columns={'npi': 'NPI'}))
                .drop(columns='NPI')
                .groupby(['quarter', groupvar], as_index=False)
                .sum()
                .assign(**{groupvar: lambda df: df[groupvar].astype('Int64')}))

    c1 = get_md_np_counts_single(
        group_inferred_q_all, practypes, 'Group Practice PAC ID')
    c2 = get_md_np_counts_single(
        group_inferred_q_all, practypes, 'my_group_id')
    c3 = get_md_np_counts_single(
        group_inferred_q_all, practypes, 'my_group_id_npi')

    counts = (group_inferred_q_all
              .merge(c1, how='left')
              .merge(c2, how='left', on=['quarter', 'my_group_id'])
              .merge(c3, how='left', on=['quarter', 'my_group_id_npi']))
    return counts.rename(columns={'MD/DO_x': 'MDDO_gppid',
                                  'NP_x': 'NP_gppid',
                                  'MD/DO_y': 'MDDO_mgi',
                                  'NP_y': 'NP_mgi',
                                  'MD/DO': 'MDDO_mginpi',
                                  'NP': 'NP_mginpi',
                                  })


def md_copractices(counts, locdata, practypes):
    counts = counts.query('quarter<"2020-01-01"')
    practypes = practypes.rename(
        columns={'npi': 'NPI', 'MD/DO': 'is_MD', 'NP': 'is_NP'})
    # Go ahead and remove the "boths" from is_MD because they mostly loo
    # like nurses. They count as both nurses and docs in the copractice
    # variable but won't be in the analysis dataset
    practypes.loc[practypes.cat == "both", "is_MD"] = 0
    # locdata = locdata.assign(quarter=lambda df: df.quarter.dt.to_timestamp())
    # counts = (counts
    #           .merge(locdata[['npi', 'quarter', 'plocstatename']]
    #                  .rename(columns={'npi': 'NPI'}))
    #           .drop(columns=['Phone Number', 'Zip Code']))
    # could maybe add in practypes from PECOS...
    counts = counts.merge(practypes)
    o = (counts
         .loc[(counts.is_MD == 1)]
         .groupby(['NPI', 'quarter', 'State'])
         [['MDDO_gppid',  'NP_gppid',  'MDDO_mgi',  'NP_mgi',
           'MDDO_mginpi', 'NP_mginpi']].max())
    return o


def get_useful_enrollment_dates(final, all_dates):
    from npi.utils.globalcache import c
    # some final additions
    npi = NPI(entities=1)
    npi.retrieve('penumdate')
    enumdates = npi.penumdate
    all_dates = (all_dates
                 .reset_index()
                 .rename(columns={'NPI': 'npi'})
                 .merge(final.npi.drop_duplicates()))

    # get rid of students
    taxes = c.get_taxcode(
        npi.src, final.npi.drop_duplicates(),
        npi.entity, npi.entities, temporal=True)

    early_date = (taxes
                  .assign(quarter=lambda df:
                          pd.PeriodIndex(df.month, freq='Q'))
                  .assign(quarter=lambda df:
                          df.quarter.dt.to_timestamp())
                  [['npi', 'ptaxcode', 'quarter']]
                  .drop_duplicates().sort_values(['npi', 'quarter'])
                  .query('ptaxcode!="390200000X"')
                  .groupby('npi')['quarter'].min())
    return enumdates, all_dates, early_date

    # # If there are no other NPIs at a date-state-zip-phone, this is a new group
    # # Should use the same group number if true at different dates
#
    # ###########################################################################
#
    # # only one group practice for an NPI; fill in
    # idcols = ['NPI']
    # updatecols = ['Group Practice PAC ID']
    # groupinfo = batch_update_cols_with_consistent_info(
    #     groupinfo, idcols, updatecols)
#
    # # only one group practice for an NPI-date; fill in
    # idcols = ['NPI', 'date']
    # updatecols = ['Group Practice PAC ID']
    # groupinfo = batch_update_cols_with_consistent_info(
    #     groupinfo, idcols, updatecols)
#
    # # only one group practice for an NPI-state-zip code; fill in
    # idcols = ['NPI', 'State', 'Zip Code']
    # updatecols = ['Group Practice PAC ID']
    # groupinfo = batch_update_cols_with_consistent_info(
    #     groupinfo, idcols, updatecols)
#
    # print('Many groups missing phone numbers: ',
    #       (groupinfo['Phone Number'] == 'nan').sum()/groupinfo.shape[0])
#
    # # Get any information that is consistent by NPI and Group Practice
    # # and update the missing information
    # # this mostly fixes phone numbers
    # idcols = ['NPI', 'Group Practice PAC ID']
    # updatecols = ['Zip Code', 'Organization legal name',
    #               'Number of Group Practice members', 'State', 'Phone Number']
    # groupinfo = batch_update_cols_with_consistent_info(
    #     groupinfo, idcols, updatecols)
#
    # idcols = ['NPI', 'State', 'Zip Code', 'Phone Number']
    # updatecols = ['Group Practice PAC ID']
    # groupinfo = batch_update_cols_with_consistent_info(
    #     groupinfo, idcols, updatecols)
#
    # idcols = ['NPI', 'Group Practice PAC ID']
    # updatecols = ['Zip Code', 'Organization legal name',
    #               'Number of Group Practice members', 'State', 'Phone Number']
    # groupinfo = batch_update_cols_with_consistent_info(
    #     groupinfo, idcols, updatecols)
#
    # # do the same fill-in for people entirely missing group practice ids
    # idcols = ['NPI']
    # updatecols = ['Zip Code', 'Organization legal name',
    #               'Number of Group Practice members', 'State', 'Phone Number']
    # missinggroup = batch_update_cols_with_consistent_info(
    #     missinggroup, idcols, updatecols)
#
    # # Can now look for people missing group numbers, to see if there
    # # is a unique match on state, zip, phone, date
    # # the majority of cases there are multiple group numbers
    # # but perhaps that's not actually right... perhaps these should be joined
    # pot_missing = (missinggroup[['State', 'Zip Code', 'Phone Number', 'date']]
    #                .drop_duplicates()
    #                .dropna()
    #                .reset_index(drop=True)
    #                .reset_index()
    #                .merge(groupinfo[['State', 'Zip Code', 'Phone Number',
    #                                  'date', 'Group Practice PAC ID',
    #                                  'Organization legal name']]
    #                       .dropna(subset=['State', 'Zip Code', 'Phone Number',
    #                                       'date', 'Group Practice PAC ID'])
    #                       .drop_duplicates()))
    # miss = (pot_missing[~pot_missing['index']
    #         .duplicated(keep=False)]
    #         .drop(columns='index'))
    # missinggroup_u = update_cols(missinggroup, miss,
    #                              ['State', 'Zip Code', 'Phone Number', 'date'])
#
    # groupinfo1 = (missinggroup_u
    #               .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
    #               .reset_index(drop=True))
    # missinggroup = (missinggroup_u
    #                 .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
    #                 .reset_index(drop=True))
    # groupinfo = (groupinfo.append(groupinfo1)
    #                       .drop_duplicates().reset_index(drop=True))
#
    # # Sole practices: number of group prac==1, npi, sometimes missing sometimes
    # # one group prac
    # solepracs = missinggroup[
    #                 missinggroup['Number of Group Practice members'] == 1
    #                 & missinggroup['Group Practice PAC ID'].isnull()]
    # # the below dropped vars all missing
    # solepracs = (solepracs
    #              .drop(columns=['date', 'Group Practice PAC ID',
    #                             'Organization legal name', 'Phone Number'])
    #              .drop_duplicates())
    # solepracs = solepracs.merge(groupinfo)
    # df_s = isolate_consistent_info(
    #     solepracs, ['NPI', 'Group Practice PAC ID'], 'Phone Number')
    # solepracs = update_cols(solepracs, df_s, ['NPI', 'Group Practice PAC ID'])
    # solepracs = (solepracs
    #              .drop(columns=['date'])
    #              .drop_duplicates()[~solepracs
    #                                 .drop(columns=['date'])
    #                                 .drop_duplicates()
    #                                 .NPI.duplicated(keep=False)])
    # missinggroup_u = update_cols(
    #     missinggroup, solepracs, ['NPI', 'Number of Group Practice members'])
    # groupinfo1 = (missinggroup_u
    #               .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
    #               .reset_index(drop=True))
    # missinggroup = (missinggroup_u
    #                 .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
    #                 .reset_index(drop=True))
    # groupinfo = (groupinfo.append(groupinfo1)
    #                       .drop_duplicates().reset_index(drop=True))
#
    # # People exist across datasets, can match on NPI, without regard to date
    # miss_unique = (missinggroup[['State', 'Zip Code', 'Phone Number', 'NPI']]
    #                .drop_duplicates())
    # miss_unique = miss_unique[~miss_unique.NPI.duplicated(keep=False)]
    # miss_unique = miss_unique.merge(groupinfo)
    # ids = ['State', 'Zip Code', 'Phone Number', 'NPI', 'Group Practice PAC ID']
    # miss_unique = (miss_unique[ids].drop_duplicates()[~miss_unique[ids]
    #                                                   .drop_duplicates()
    #                                                   .NPI
    #                                                   .duplicated(keep=False)])
    # missinggroup_u = update_cols(missinggroup,
    #                              miss_unique,
    #                              ['NPI', 'State', 'Zip Code', 'Phone Number'])
    # groupinfo1 = (missinggroup_u
    #               .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
    #               .reset_index(drop=True))
    # missinggroup = (missinggroup_u
    #                 .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
    #                 .reset_index(drop=True))
    # groupinfo = (groupinfo.append(groupinfo1)
    #                       .drop_duplicates().reset_index(drop=True))
#
    # # similar, slightly different approach
    # ids = ['NPI', 'Number of Group Practice members', 'Zip Code',
    #        'Phone Number', 'State']
    # d = missinggroup[ids].drop_duplicates().merge(
    #     groupinfo[ids + ['Group Practice PAC ID']].drop_duplicates())
#
    # d = d[~d.NPI.duplicated(keep=False)]
    # missinggroup_u = update_cols(missinggroup, d, ids)
#
    # groupinfo1 = (missinggroup_u
    #               .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
    #               .reset_index(drop=True))
    # missinggroup = (missinggroup_u
    #                 .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
    #                 .reset_index(drop=True))
    # groupinfo = (groupinfo.append(groupinfo1)
    #                       .drop_duplicates().reset_index(drop=True))
#
    # missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)
#
    # # Make some new groupnos for unmatched sole proprietors
    # m = missinggroup[missinggroup['Number of Group Practice members'] == 1]
    # m = (m[['NPI', 'Zip Code', 'State']]
    #      .drop_duplicates()
    #      .reset_index(drop=True)
    #      .reset_index())
    # m['Group Practice PAC ID'] = (m['index'] + 200000000000).astype('Int64')
    # missinggroup_u = update_cols(
    #     missinggroup, m.drop(columns='index'), ['NPI', 'State', 'Zip Code'])
#
    # groupinfo1 = (missinggroup_u
    #               .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
    #               .reset_index(drop=True))
    # missinggroup = (missinggroup_u
    #                 .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
    #                 .reset_index(drop=True))
    # groupinfo = (groupinfo.append(groupinfo1)
    #                       .drop_duplicates().reset_index(drop=True))
    # missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)
#
    # ids = ['State', 'Zip Code', 'Phone Number', 'date']
    # m = (missinggroup[ids]
    #      .drop_duplicates()
    #      .merge(groupinfo[ids + ['Group Practice PAC ID']].drop_duplicates()))
#
    # new_groups = m[~m[ids].duplicated(keep=False)]
    # missinggroup_u = update_cols(
    #     missinggroup, new_groups, ids)
#
    # groupinfo1 = (missinggroup_u
    #               .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
    #               .reset_index(drop=True))
    # missinggroup = (missinggroup_u
    #                 .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
    #                 .reset_index(drop=True))
    # groupinfo = (groupinfo.append(groupinfo1)
    #                       .drop_duplicates().reset_index(drop=True))
#
    # missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)
#
    # # explanation here. match only on zip and phone
#
    # k = (missinggroup[['Zip Code', 'Phone Number']]
    #      .drop_duplicates()
    #      .merge(groupinfo, how='left', indicator=True))
    # # k2 = k.query('_merge=="left_only"')
    # # k2 = k2.reset_index(drop=True).reset_index()
    # # k2['Group Practice PAC ID'] = (
    # #   k2['index'] + 300000000000).astype('Int64'))
#
    # ids = ['Zip Code', 'Phone Number']
    # k1 = (k.query('_merge=="both"')[ids + ['Group Practice PAC ID']]
    #        .drop_duplicates())
    # zip_phone_pacids = k1[~k1['Group Practice PAC ID'].duplicated(keep=False)]
    # missinggroup_u = update_cols(
    #     missinggroup, zip_phone_pacids, ids)
#
    # groupinfo1 = (missinggroup_u
    #               .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
    #               .reset_index(drop=True))
    # missinggroup = (missinggroup_u
    #                 .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
    #                 .reset_index(drop=True))
    # groupinfo = (groupinfo.append(groupinfo1)
    #                       .drop_duplicates().reset_index(drop=True))
#
    # missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)
#
    # # Fill in for anyone missing, where you are the same location if you
    # # have the same phone number and zip at the same date
#
    # k2 = (missinggroup[['Zip Code', 'Phone Number', 'date']]
    #       .dropna()
    #       .drop_duplicates()
    #       .reset_index(drop=True).reset_index())
    # k2['Group Practice PAC ID'] = (k2['index'] + 300000000000).astype('Int64')
    # k2 = k2.drop(columns='index')
    # missinggroup_u = update_cols(
    #     missinggroup, k2, ['Zip Code', 'Phone Number', 'date'])
    # groupinfo1 = (missinggroup_u
    #               .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
    #               .reset_index(drop=True))
    # missinggroup = (missinggroup_u
    #                 .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
    #                 .reset_index(drop=True))
    # groupinfo = (groupinfo.append(groupinfo1)
    #                       .drop_duplicates().reset_index(drop=True))
    # missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)
#
    # isid(missinggroup, ['NPI', 'date', 'Zip Code', 'State'])
#
    # # Finally, make up ids for nonmatches
    # k3 = missinggroup.reset_index(drop=True).reset_index()
    # k3['Group Practice PAC ID'] = (k3['index'] + 400000000000).astype('Int64')
    # k3 = k3.drop(columns='index')
#
    # missinggroup_u = update_cols(
    #     missinggroup, k3, ['NPI', 'Zip Code', 'State', 'date'])
    # groupinfo1 = (missinggroup_u
    #               .loc[missinggroup_u['Group Practice PAC ID'].notnull()]
    #               .reset_index(drop=True))
    # missinggroup = (missinggroup_u
    #                 .loc[missinggroup_u['Group Practice PAC ID'].isnull()]
    #                 .reset_index(drop=True))
    # groupinfo = (groupinfo.append(groupinfo1)
    #                       .drop_duplicates().reset_index(drop=True))
    # missinggroup = missinggroup.drop_duplicates().reset_index(drop=True)
    # assert missinggroup.shape[0] == 0
    # return groupinfo

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


def mds_nps_practype_npi_pecos():
    """
    This cleans and sanitizes the practitioner type such that I include
    information
    from both the NPI and PECOS on a practitioner's type, especially since
    there
    were a number of miscoded NPs as MDs.
    """

    npi = NPI(entities=1)
    npi.retrieve('practitioner_type')
    # get about 1.5 million MDs and NPs
    s = npi.practitioner_type.set_index('npi')[['MD/DO', 'NP']].sum(axis=1) > 0
    mds_nps = s[s].reset_index().drop(columns=0)
    practypes = npi.practitioner_type.merge(mds_nps)[['npi', 'MD/DO', 'NP']]

    md_np_cred = (npi
                  .credentials
                  .query('cat=="MD/DO" or cat=="NP"')[['npi', 'cat']]
                  .drop_duplicates())
    md_np_cred_both = md_np_cred[md_np_cred.npi.duplicated(keep=False)]
    md_np_cred = md_np_cred[~md_np_cred.npi.duplicated(keep=False)]
    practypes = practypes.merge(md_np_cred, how='left')

    pecos = PECOS(['NPI', 'Last Name', 'First Name', 'Middle Name',
                   'Suffix', 'State', 'Zip Code', 'Phone Number'])
    pecos.retrieve('practitioner_type')
    # 1. Select MD/DO and NPs from either NPI or PECOS
    practitioners = pecos.practitioner_type
    mddo = (practitioners
            .query('Credential=="MD/DO" or '
                   'Credential=="MD" or Credential=="DO"')
            .NPI.drop_duplicates())
    nps = (practitioners.loc[(practitioners['Primary specialty']
                              == "NURSE PRACTITIONER")
                             | (practitioners['Credential'] == 'NP')]
           .NPI.drop_duplicates())

    pecos_practypes = (pd
                       .DataFrame(mddo.astype('Int64'))
                       .assign(MDDO=1)
                       .merge(pd.DataFrame(nps.astype('Int64')).assign(NP=1),
                              how='outer')
                       .fillna(0)
                       .set_index('NPI')
                       .astype('int')
                       .reset_index())

    practypes = (practypes
                 .assign(NPI=practypes.npi.astype('Int64'))
                 .rename(columns={'MD/DO': 'MDDO'})
                 .drop(columns='npi'))

    m = practypes.merge(pecos_practypes, how='outer')
    practypes_c = (m[~m.NPI.duplicated(keep=False)]
                   .query('(MDDO==1 & NP==1)==0')
                   .query('(cat=="NP" & MDDO==1)==0')
                   .query('(cat=="MD/DO" & NP==1)==0'))
    practypes_c.loc[practypes_c.MDDO == 1, 'cat'] = "MD/DO"
    practypes_c.loc[practypes_c.NP == 1, 'cat'] = "NP"

    sums = (m
            .merge(practypes_c, how='left', indicator=True)
            .query('_merge=="left_only"')
            .drop(columns="_merge")
            .sort_values('NPI')
            .groupby('NPI', as_index=False)
            .sum()
            .merge(md_np_cred.rename(columns={'npi': 'NPI'})))

    additions = (sums
                 .query('NP==1 & (MDDO==1 | MDDO==2) & cat=="MD/DO"')
                 .assign(MDDO=1, NP=0)
                 .append(sums
                         .query('MDDO==1 & (NP==1 | NP==2) & cat=="NP"')
                         .assign(MDDO=0, NP=1))
                 .drop_duplicates())
    practypes_c = practypes_c.append(additions).drop_duplicates()
    isid(practypes_c, ['NPI'])

    final_adds = (m
                  .merge(practypes_c.NPI, how='left', indicator=True)
                  .query('_merge=="left_only"')
                  .drop(columns="_merge")
                  .sort_values('NPI').groupby('NPI', as_index=False)
                  .sum()
                  .loc[lambda df: ~df.NPI.isin(md_np_cred_both.npi)])
    final_adds.loc[lambda df: df.MDDO > df.NP, 'cat']='MD/DO'
    final_adds.loc[lambda df: df.MDDO < df.NP, 'cat']='NP'
    final_adds.loc[final_adds.cat == 'NP', 'NP'] = 1
    final_adds.loc[final_adds.cat == 'NP', 'MDDO'] = 0
    final_adds.loc[final_adds.cat == 'MD/DO', 'MDDO'] = 1
    final_adds.loc[final_adds.cat == 'MD/DO', 'NP'] = 0
    practypes_c = practypes_c.append(final_adds.dropna())
    both = (final_adds
            .loc[final_adds.cat.isnull()]
            .append(pd.DataFrame(md_np_cred_both.npi
                                 .rename('NPI')
                                 .drop_duplicates())))
    practypes_c = practypes_c.append(both.assign(MDDO=1, NP=1, cat="both"))
    isid(practypes_c, ['NPI'])

    assert (mds_nps.npi.append(mddo).append(nps).drop_duplicates().shape[0]
            == practypes_c.NPI.drop_duplicates().shape[0])

    practypes_c = (practypes_c
                   .rename(columns={'NPI': 'npi', 'MDDO': 'MD/DO'})
                   [['npi',  'MD/DO',  'NP', 'cat']])
    mds_nps = practypes_c[['npi']]
    return mds_nps, practypes_c


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

def isolate_consistent_info(df, idcols, targetcols):
    """
    If a column is consistent among the id variables,
    this retrieves that info, unique at the idcols
    level. we assume this is the master information

    If targetcols is a list, only need some of them to be non-missing
    NOT ALL. Ie targetcols contain some information as a group
    """
    if isinstance(targetcols, str):
        df = (df[idcols + [targetcols]]
              .drop_duplicates()
              .dropna()
              .loc[lambda d: d[targetcols] != 'nan']
              .reset_index(drop=True))
    elif isinstance(targetcols, list):
        df = fully_blank_rows_in_varlist(
            df[idcols + targetcols].drop_duplicates(),
            targetcols, inverse=True)
    df = df[~df[idcols].duplicated(keep=False)]
    isid(df, idcols)
    df = df.reset_index(drop=True)
    return df


def update_cols(df, update_df, idcols):
    """
    Fills in any missing details of a column with new information,
    merged in on the idcols level
    """
    df = df.merge(update_df, on=idcols, how='left')
    updatecols = [x for x in update_df.columns if x not in idcols]
    for col in updatecols:
        cond = ((df[f'{col}_x'].isnull())
                | (df[f'{col}_x'] == 'nan')) & (df[f'{col}_y'].notnull())
        s1 = cond.sum()
        s2 = df.shape[0]
        print('For col %s, replacing %s out of %s lines, or %s percent' %
              (col, s1, s2, 100*s1/s2))
        df[col] = df[f'{col}_x'].fillna(df[f'{col}_y'])
        df.loc[cond, col] = df[f'{col}_y']
        df = df.drop(columns=[f'{col}_x', f'{col}_y'])
    return df


def batch_update_cols_with_consistent_info(df, idcols, updatecols):
    """
    update columns of df per the above two functions

    return a DF with duplicates dropped
    """
    for col in updatecols:
        print('updating column:', col)
        if any((df[col].isnull()) | (df[col] == 'nan')):
            df_u = isolate_consistent_info(df, idcols, col)
            if not df_u.empty:
                df = update_cols(df, df_u, idcols)

    return df.drop_duplicates().reset_index(drop=True)


def fully_blank_rows_in_varlist(df, varlist, inverse=False):
    return (df[(((df[varlist].isnull()) | (df[varlist] == 'nan')).all(1))]
            if not inverse
            else
            df[~(((df[varlist].isnull()) | (df[varlist] == 'nan')).all(1))])
