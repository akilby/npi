import os

import pandas as pd

WEB_PATH = 'https://data.cms.gov/api/views/%s/rows.csv?accessType=DOWNLOAD&api_foundry=true'
RAW_DIR = '/work/akilby/npi/raw_opi'

file_id_dict = {2017: 'sakz-a2rp',
                2016: '6wg9-kwip',
                2015: '6i2k-7h8p',
                2014: 'e4ka-3ncx',
                2013: 'yb2j-f3fp'}


def medicare_opioid_paths(yearlist=range(2013, 2018), src=RAW_DIR):
    file_dict = {}
    if src == WEB_PATH:
        for year, key in file_id_dict.items():
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
