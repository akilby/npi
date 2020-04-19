import pandas as pd

from ..constants import PART_B_STUB, PART_B_STUB_SUM
from ..download.medicare import (list_part_b_files, list_part_d_files,
                                 list_part_d_opi_files)
from . import PARTB_COLNAMES


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


# Notes: the drug files contain obs at the doctor, and doctor-drug level, if that doctor
# has greater than 10 claims. Presumably there should be more docs in the short file
# than the long file.

def main():
    drug = part_d_files(Drug=False)
    drug.total_claim_count.isnull().sum()
    (drug.total_claim_count == 0).sum()
    (drug.total_claim_count == 10).sum()
    (drug.total_claim_count == 11).sum()
    (drug.total_claim_count == 12).sum()

    drug_long = part_d_files(Drug=True)
    drug_long.total_claim_count.isnull().sum()
    (drug_long.total_claim_count == 0).sum()
    (drug_long.total_claim_count == 10).sum()
    (drug_long.total_claim_count == 11).sum()
    (drug_long.total_claim_count == 12).sum()

    df_sum = part_b_files(summary=True)
    df = part_b_files()


# s=time.time()
# df2=part_b_files(columns=['National Provider Identifier'])
# print(time.time()-s)

# s=time.time()
# df=part_b_files()
# print(time.time()-s)
