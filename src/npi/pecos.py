"""

elig:
drop if old grad year, or if they drop out after high historical claims (if
only a few claims, maybe cant determine)"
people can move to private practice..."

could also try; retire them when they hit retirement age, unless evidence
they still practice

"""

import pandas as pd

from .download.medical_schools import final_data_path as med_school_path
from .npi import expand_names_in_sensible_ways
from .process.medicare import part_b_files, part_d_files
from .process.physician_compare import physician_compare_select_vars
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
    pc = physician_compare_select_vars([],
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
    return (pc_engage
            .merge(partd_engage, how='outer')
            .merge(partb_engage, how='outer')
            .convert_dtypes({x: 'Int64' for x in pc_engage.columns}))


def medical_school(include_web_scraped=True):
    """
    Returns medical schools and graduation dates at the NPI level
    Has been unique-ified by dropping "other" entries, and then
    randomly choosing between duplicates if one isn't other
    Also has the option of bringing in approx. 127 entries from the
    web-scraped database.
    """
    cols = ['Medical school name', 'Graduation year']
    med_school = physician_compare_select_vars(cols)
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
