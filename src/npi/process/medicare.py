import pandas as pd

from ..constants import PART_B_STUB, PART_B_STUB_SUM
from ..download.medicare import (list_part_b_files, list_part_d_files,
                                 list_part_d_opi_files)
from ..utils.utils import isid
from . import PARTB_COLNAMES


def part_d_files(summary=False, usecols=None, years=range(2013, 2018)):
    """
    summary=False -> Drug=True gives the larger/longer/more detailed files
    summary=True -> Drug=False gives the summary file
    """
    Drug = True if not summary else False
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


def part_d_info():
    print('Note: both the Part D files and the Part B files have a long file '
          'and a short (summary) file. The long file is at the physician-drug'
          ' or physician-procedure level, whereas the short file is a summary '
          'at the physician level. An observation is censored if it has less '
          'than 11 drug claims or less than 11 beneficiaries comprising it,'
          ' so the long files will have fewer physicians than the short files')
    drug = part_d_files(summary=True, usecols=['npi', 'total_claim_count'])
    print('Short Part D files:')
    isid(drug, ['npi', 'Year'], noisily=True)
    print('Missing total claim count:\t%s'
          % drug.total_claim_count.isnull().sum())
    print('0 total claims:\t\t\t%s' % (drug.total_claim_count == 0).sum())
    print('10 total claims:\t\t%s' % (drug.total_claim_count == 10).sum())
    print('11 total claims:\t\t%s' % (drug.total_claim_count == 11).sum())
    print('12 total claims:\t\t%s' % (drug.total_claim_count == 12).sum())

    drug_long = part_d_files(usecols=['npi', 'drug_name', 'generic_name',
                                      'total_claim_count'])
    print('Long Part D files:')
    isid(drug_long, ['npi', 'Year', 'drug_name', 'generic_name'], noisily=True)
    print('Missing total claim count:\t%s'
          % drug_long.total_claim_count.isnull().sum())
    print('0 total claims:\t\t\t%s' % (drug_long.total_claim_count == 0).sum())
    print('10 total claims:\t\t%s' % (drug_long.total_claim_count == 10).sum())
    print('11 total claims:\t\t%s' % (drug_long.total_claim_count == 11).sum())
    print('12 total claims:\t\t%s' % (drug_long.total_claim_count == 12).sum())

    print("Merging shows that all enrollment information is present"
          " in the short Part D dataframe. About 22% of physicians who "
          "prescribe in Part D do not do enough prescribing to show up in the "
          "detailed files")
    partd = drug.merge(
        drug_long.groupby(['npi', 'Year'], as_index=False).sum(),
        on=['npi', 'Year'],
        how='outer',
        indicator=True)
    print(partd._merge.value_counts())
    partd = (partd.sort_values(['npi', 'Year'])
                  .reset_index(drop=True)
                  .drop(columns='_merge')
                  .rename(columns={'total_claim_count_x':
                                   'total_claim_count',
                                   'total_claim_count_y':
                                   'total_claim_count_drug_detail'}))
    print('Opioid files:')
    opi = part_d_opi_files()
    print('Note: opioid files are at the same level of observation'
          ' as the short files')
    assert all(
        drug.sort_values(['npi', 'Year']).set_index(['npi', 'Year']).index
        == (opi.rename(columns={'NPI': 'npi'})
               .sort_values(['npi', 'Year'])
               .set_index(['npi', 'Year']).index)
        )
    print('Opioid files do have zeros and nulls')
    opi = opi[['NPI', 'Year', 'Total Claim Count', 'Opioid Claim Count']]
    print('Missing total claim count:\t%s'
          % opi['Opioid Claim Count'].isnull().sum())
    print('0 total claims:\t\t\t%s' % (opi['Opioid Claim Count'] == 0).sum())
    print('10 total claims:\t\t%s' % (opi['Opioid Claim Count'] == 10).sum())
    print('11 total claims:\t\t%s' % (opi['Opioid Claim Count'] == 11).sum())
    print('12 total claims:\t\t%s' % (opi['Opioid Claim Count'] == 12).sum())

    print('of the 5,518,978 person-years in the Part D data, 1,430,428 are '
          'listed with no opioids and 1,599,355 with null (meaning 1-10)')
    opi.shape
    opi[opi['Opioid Claim Count'] == 0].shape
    opi[opi['Opioid Claim Count'].isnull()].shape

    print('of the 1430428 with a 0 value, about 62% show up in both the '
          'short and long file, whereas 38% show up in only the short file')
    print(
        opi[opi['Opioid Claim Count'] == 0]
        .rename(columns={'NPI': 'npi'})
        .merge(drug_long, how='left', indicator=True)
        [['npi', 'Year', '_merge']]
        .drop_duplicates()
        ._merge.value_counts())
    print('of the 1599355 with a null value, about 2/3 show up in both the '
          'short and long file, whereas 1/3 show up in only the short file')
    print(
        opi[opi['Opioid Claim Count'].isnull()]
        .rename(columns={'NPI': 'npi'})
        .merge(drug_long, how='left', indicator=True)
        [['npi', 'Year', '_merge']]
        .drop_duplicates()
        ._merge.value_counts())

    print('Conclusion: there are real zeros in the opioid files! If '
          'someone in general prescribes enough overall drugs to show up in '
          'the Part D data (10 claims over all drugs total), then a zero '
          'listed for them for opioids is a true zero. Confirmed in the '
          'methodology documents: "opioid_claim_count – Total claims of opioid'
          ' drugs, including refills. The opioid_claim_count is suppressed '
          'when opioid_claim_count is between 1 and 10."')

    print('Finally, the opi files appear to be just a convenience cut of the '
          'Part D summary file (would need to check all columns to verify):')
    drug2 = part_d_files(summary=True, usecols=['npi', 'total_claim_count',
                                                'opioid_claim_count',
                                                'la_opioid_claim_count'])
    opi = part_d_opi_files()
    opi = opi[['NPI', 'Year', 'Total Claim Count', 'Opioid Claim Count',
               'Long-Acting Opioid Claim Count']]
    print(opi.shape)
    print(drug2.shape)
    print(opi.rename(columns={'NPI': 'npi',
                              'Total Claim Count': 'total_claim_count',
                              'Opioid Claim Count': 'opioid_claim_count',
                              'Long-Acting Opioid Claim Count':
                              'la_opioid_claim_count'})
          .merge(drug2).shape)


def part_b_info():
    print('Note: both the Part D files and the Part B files have a long file '
          'and a short (summary) file. The long file is at the physician-drug'
          ' or physician-procedure level, whereas the short file is a summary '
          'at the physician level. An observation is censored if it has less '
          'than 11 drug claims or less than 11 beneficiaries comprising it,'
          ' so the long files will have fewer physicians than the short files')

    df_sum = part_b_files(summary=True,
                          columns=['National Provider Identifier',
                                   'Number of Medicare Beneficiaries'])

    print('Short Part B files:')
    isid(df_sum, ['National Provider Identifier', 'Year'], noisily=True)
    print('Missing total claim count:\t%s'
          % df_sum['Number of Medicare Beneficiaries'].isnull().sum())
    print('0 total claims:\t\t\t%s'
          % (df_sum['Number of Medicare Beneficiaries'] == 0).sum())
    print('10 total claims:\t\t%s'
          % (df_sum['Number of Medicare Beneficiaries'] == 10).sum())
    print('11 total claims:\t\t%s'
          % (df_sum['Number of Medicare Beneficiaries'] == 11).sum())
    print('12 total claims:\t\t%s'
          % (df_sum['Number of Medicare Beneficiaries'] == 12).sum())

    df = part_b_files(columns=['National Provider Identifier', 'HCPCS Code',
                               'Place of Service',
                               'Number of Medicare Beneficiaries'])
    print('Long Part B files:')
    idcols = ['National Provider Identifier', 'HCPCS Code',
              'Place of Service', 'Year']
    isid(df, idcols, noisily=True)
    print('Missing total claim count:\t%s'
          % df['Number of Medicare Beneficiaries'].isnull().sum())
    print('0 total claims:\t\t\t%s'
          % (df['Number of Medicare Beneficiaries'] == 0).sum())
    print('10 total claims:\t\t%s'
          % (df['Number of Medicare Beneficiaries'] == 10).sum())
    print('11 total claims:\t\t%s'
          % (df['Number of Medicare Beneficiaries'] == 11).sum())
    print('12 total claims:\t\t%s'
          % (df['Number of Medicare Beneficiaries'] == 12).sum())

    print('The three nulls are mistakes:')
    print(df[df['Number of Medicare Beneficiaries'].isnull()])
    df = df[~df['Number of Medicare Beneficiaries'].isnull()]

    print("Merging shows that all enrollment information is present"
          " in the short Part B dataframe. About 5% of physicians who "
          "show up in Part B do not do enough procedures to show up in the "
          "detailed procedure files")
    partb = df_sum.merge(
        df.groupby(['National Provider Identifier', 'Year'], as_index=False)
        .sum(),
        on=['National Provider Identifier', 'Year'],
        how='outer',
        indicator=True)
    print(partb._merge.value_counts())
    print('fraction:', 299738/(299738+5730647))
