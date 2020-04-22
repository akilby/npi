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
from .process.medicare import part_b_files, part_d_files
from .process.physician_compare import physician_compare_select_vars
from .utils.utils import isid


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