import pandas as pd

from ..constants import PART_B_STUB, PART_B_STUB_SUM
from ..download.medicare import (list_part_b_files, list_part_d_files,
                                 list_part_d_opi_files)
from . import PARTB_COLNAMES
from .physician_compare import process_vars


def part_d_files(Drug=True, usecols=None, years=range(2013, 2018)):
    """
    Drug=True gives the larger/longer/more detailed files
    Drug=False gives the summary file
    """
    return pd.concat([pd.read_csv(x, usecols=usecols, sep='\t').assign(Year=y)
                      for (x, y) in list_part_d_files(Drug=Drug)
                      if y in years])


def part_d_opi_files(usecols=None, years=range(2013, 2018)):
    return pd.concat([pd.read_csv(x, usecols=usecols).assign(Year=y)
                      for (x, y) in list_part_d_opi_files() if y in years])


def part_b_files(summary=False,
                 years=range(2012, 2018),
                 coldict=PARTB_COLNAMES,
                 columns=None):
    # Columns takes a list of destination column names, and searches
    # through the rename dicts to find the original column name
    filestub = PART_B_STUB_SUM if summary else PART_B_STUB
    params = search_column_rename_dict_for_colnames(columns, coldict)
    return pd.concat([pd.read_csv(x, **params)
                        .assign(Year=y)
                        .rename(columns=coldict)
                        .rename(str.strip, axis='columns')
                        .rename(columns=coldict)
                      for (x, y) in list_part_b_files(filestub) if y in years])


def search_column_rename_dict_for_colnames(columns, coldict):
    if columns:
        cols = [key for key, val in coldict.items() if val in columns]
        params = dict(usecols=lambda x: x in cols or x.strip() in cols)
    else:
        params = {}
    return params


def main():
    # Notes: the drug files contain obs at the doctor, and doctor-drug level,
    # if that doctor
    # has greater than 10 claims. Presumably there should be more docs in the
    # short file
    # than the long file.
    drug = part_d_files(Drug=False, usecols=['npi', 'total_claim_count'])
    print(drug.total_claim_count.isnull().sum())
    print((drug.total_claim_count == 0).sum())
    print((drug.total_claim_count == 10).sum())
    print((drug.total_claim_count == 11).sum())
    print((drug.total_claim_count == 12).sum())

    drug_long = part_d_files(Drug=True, usecols=['npi', 'total_claim_count'])
    print(drug_long.total_claim_count.isnull().sum())
    print((drug_long.total_claim_count == 0).sum())
    print((drug_long.total_claim_count == 10).sum())
    print((drug_long.total_claim_count == 11).sum())
    print((drug_long.total_claim_count == 12).sum())

    print(drug.merge(drug_long.groupby(['npi', 'Year']).sum().reset_index(),
                     on=['npi', 'Year'],
                     how='outer',
                     indicator=True)._merge.value_counts())
    partd = (drug.merge(drug_long.groupby(['npi', 'Year']).sum().reset_index(),
                        on=['npi', 'Year'],
                        how='left')
                 .sort_values(['npi', 'Year'])
                 .reset_index(drop=True)
                 .rename(columns={'total_claim_count_x':
                                  'total_claim_count',
                                  'total_claim_count_y':
                                  'total_claim_count_drug_detail'}))
    # Note: opioid files don't add any more information; they have the same
    # total claim count as the aggregated files

    df_sum = part_b_files(summary=True,
                          columns=['National Provider Identifier',
                                   'Number of Medicare Beneficiaries'])
    print((df_sum['Number of Medicare Beneficiaries'].isnull()).sum())
    print((df_sum['Number of Medicare Beneficiaries'] == 0).sum())
    print((df_sum['Number of Medicare Beneficiaries'] == 10).sum())
    print((df_sum['Number of Medicare Beneficiaries'] == 11).sum())
    print((df_sum['Number of Medicare Beneficiaries'] == 12).sum())
    df = part_b_files(columns=['National Provider Identifier',
                               'Number of Medicare Beneficiaries'])
    print((df['Number of Medicare Beneficiaries'].isnull()).sum())
    print((df['Number of Medicare Beneficiaries'] == 0).sum())
    print((df['Number of Medicare Beneficiaries'] == 10).sum())
    print((df['Number of Medicare Beneficiaries'] == 11).sum())
    print((df['Number of Medicare Beneficiaries'] == 12).sum())
    partb = (df_sum.merge(df.groupby(['National Provider Identifier', 'Year'])
                            .sum().reset_index(),
                          on=['National Provider Identifier', 'Year'],
                          how='left')
                   .sort_values(['National Provider Identifier', 'Year'])
                   .reset_index(drop=True)
                   .rename(columns={'Number of Medicare Beneficiaries_x':
                                    'Number of Medicare Beneficiaries',
                                    'Number of Medicare Beneficiaries_y':
                                    'Number of Medicare Beneficiaries_detail'})
             )
    claims = (partb.rename(columns={'National Provider Identifier': 'npi'})
                   .merge(partd, how='outer'))
    # ADD PHYSICIAN COMPARE
    cols = ['Medical school name', 'Graduation year',
            'Group Practice PAC ID', 'Number of Group Practice members']
    pc = process_vars(cols, drop_duplicates=False, date_var=True)
    grad_years = (pc.groupby(['NPI', 'Graduation year'])
                    .size()
                    .reset_index()
                    .sort_values(['NPI', 0])
                    .groupby('NPI')
                    .last()
                    .drop(columns=0)
                    .reset_index())
    pc = (pc[['NPI', 'date']].assign(Year=pc.date.dt.year)
                             .drop(columns='date')
                             .drop_duplicates()
                             .assign(physician_compare=1))
    medicare = (claims.merge(pc.rename(columns={'NPI': 'npi'}), how='outer')
                      .sort_values(['npi', 'Year'])
                      .merge(grad_years.rename(columns={'NPI': 'npi'}),
                             how='left', on='npi'))
    medicare['Graduation year'] = medicare['Graduation year'].astype('Int64')
    medicare = (medicare.merge(medicare[['npi', 'Year']].groupby('npi').max()
                                                        .reset_index()
                                                        .rename(
                                                            columns={'Year':
                                                                     'MaxYear'}
                                                                     )))
