import os

import pandas as pd

from ..utils.utils import wget_checkfirst

WEB_PATH = 'https://data.cms.gov/api/views/%s/rows.csv?accessType=DOWNLOAD&api_foundry=true'
RAW_DIR_OPI = '/work/akilby/npi/raw_medicare/raw_opi'
RAW_DIR = '/work/akilby/npi/raw_medicare'

WEB_PATH2 = 'https://data.cms.gov/api/views/%s/rows.csv?accessType=DOWNLOAD&api_foundry=true'

file_id_dict_opi = {2017: 'sakz-a2rp',
                    2016: '6wg9-kwip',
                    2015: '6i2k-7h8p',
                    2014: 'e4ka-3ncx',
                    2013: 'yb2j-f3fp'}

file_id_dict_puf = {2017: 'fs4p-t5eq'}

wget_checkfirst(WEB_PATH2 % 'n5qc-ua94', to_dir=RAW_DIR)
wget_checkfirst(WEB_PATH2 % '77gb-8z53', to_dir=RAW_DIR)
wget_checkfirst(WEB_PATH2 % 'psut-35i4', to_dir=RAW_DIR)
wget_checkfirst(WEB_PATH2 % 'fs4p-t5eq', to_dir=RAW_DIR)
wget_checkfirst(WEB_PATH2 % 'mj5m-pzi6', to_dir=RAW_DIR)


def medicare_opioid_paths(yearlist=range(2013, 2018), src=RAW_DIR):
    file_dict = {}
    if src == WEB_PATH:
        for year, key in file_id_dict_opi.items():
            if year in yearlist:
                file_dict[year] = src % key
    else:
        # folder with years in filenames
        files = os.listdir(src)
        for year in yearlist:
            m = [file for file in files if str(year) in file]
            assert len(m) == 1
            file_dict[year] = os.path.join(src, m[0])
    return file_dict


def medicare_opioid_rx(file_dict):
    df_main = pd.DataFrame()
    for year, path in file_dict.items():
        df = pd.read_csv(path)
        df['Year'] = year
        df_main = pd.concat([df_main, df], axis=0)
    return df_main

li = [
      '/work/akilby/npi/raw_medicare/Medicare_Physician_and_Other_Supplier_National_Provider_Identifier__NPI__Aggregate_Report__Calendar_Year_2017.csv',
      '/work/akilby/npi/raw_medicare/Medicare_Provider_Utilization_and_Payment_Data__2017_Part_D_Prescriber.csv',
      '/work/akilby/npi/raw_medicare/Medicare_Provider_Utilization_and_Payment_Data__Part_D_Prescriber_Summary_Table_CY2017.csv',
      '/work/akilby/npi/raw_medicare/Medicare_Provider_Utilization_and_Payment_Data__Physician_and_Other_Supplier_PUF_CY2017.csv',
      '/work/akilby/npi/raw_medicare/raw_opi/Medicare_Part_D_Opioid_Prescriber_Summary_File_2017.csv'
      ]
u = lambda x: x in ['npi', 'NPI', 'National Provider Identifier']
active_2017 = pd.concat([pd.read_csv(filepath, usecols=u).drop_duplicates().rename(columns={'National Provider Identifier': 'npi', 'NPI': 'npi'}) for filepath in li])
