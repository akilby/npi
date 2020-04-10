import glob
import os
import random
import time

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from ..npi.npi import NPI
from ..utils.utils import longprint

med_school_partials_folder = '/work/akilby/npi/data/medical_schools'
raw_folder = '/work/akilby/npi/raw_web'
final_data_path = '/work/akilby/npi/data/medical_schools/medical_schools.csv'


class HTMLTableParser:

    def parse_url(self, read_path, save_path=None):
        if read_path.startswith('http'):
            response = requests.get(read_path)
            soup = BeautifulSoup(response.text, 'lxml')
        else:
            with open(read_path, 'r') as f:
                text = f.read()
                soup = BeautifulSoup(text, 'lxml')

        if save_path:
            with open(save_path, 'w') as f:
                f.write(response.text)

        return [self.parse_html_table(table)
                for table in soup.find_all('table')]

    def parse_html_table(self, table):
        n_columns = 0
        n_rows = 0
        column_names = []

        # Find number of rows and columns
        # we also find the column titles if we can
        for row in table.find_all('tr'):

            # Determine the number of rows in the table
            td_tags = row.find_all('td')
            if len(td_tags) > 0:
                n_rows += 1
                if n_columns == 0:
                    # Set the number of columns for our table
                    n_columns = len(td_tags)

            # Handle column names if we find them
            th_tags = row.find_all('th')
            if len(th_tags) > 0 and len(column_names) == 0:
                for th in th_tags:
                    column_names.append(th.get_text())

        columns = (column_names if len(column_names) > 0
                   else range(0, n_columns))
        df = pd.DataFrame(columns=columns,
                          index=range(0, n_rows))
        row_marker = 0
        for row in table.find_all('tr'):
            column_marker = 0
            columns = row.find_all('td')
            for column in columns:
                df.iat[row_marker, column_marker] = column.get_text()
                column_marker += 1
            if len(columns) > 0:
                row_marker += 1

        return df


def npi_data_scraped(npi, table, src):
    medical_school = table[0][table[0][0] == "Medical School Name"][1].values
    grad_year = table[0][table[0][0] == "Graduation Year"][1].values
    medical_school = medical_school if medical_school.size > 0 else np.nan
    grad_year = grad_year if grad_year.size > 0 else np.nan
    df = pd.DataFrame({'npi': [npi],
                       'medical_school': medical_school,
                       'grad_year': grad_year,
                       'source': src})
    return df


def retrieve_npis(npi_list, save_path='/work/akilby/npi/raw_web/',
                  quietly=False):
    i = 0
    df_long = []
    not_found = []
    hp = HTMLTableParser()
    for npi in npi_list:
        i += 1
        save_file_path = '%snpino_%s.txt' % (save_path, npi)
        save_file_path2 = '%snpiprofile_%s.txt' % (save_path, npi)
        if (not os.path.exists(save_file_path)
                or not os.path.exists(save_file_path2)):
            time.sleep(random.uniform(.5, 2))
        if os.path.exists(save_file_path):
            table = hp.parse_url(save_file_path)
        else:
            table = hp.parse_url('https://npino.com/npi/%s' % npi,
                                 save_path=save_file_path)
        if os.path.exists(save_file_path2):
            table2 = hp.parse_url(save_file_path2)
        else:
            table2 = hp.parse_url('https://npiprofile.com/npi/%s' % npi,
                                  save_path=save_file_path2)
        if table or table2:
            if not quietly:
                print('NPI %s: downloaded' % npi)
            else:
                if round(i, -3) == i:
                    print(i)
        if table:
            df1 = npi_data_scraped(npi, table, 'npino')
            df_long.append(df1)
        if table2:
            df2 = npi_data_scraped(npi, table2, 'npiprofile')
            df_long.append(df2)
        if not table and not table2:
            print('NPI %s: not found' % npi)
            not_found.append(npi)
    if df_long:
        return pd.concat(df_long), not_found
    else:
        return pd.DataFrame(), not_found


def update_db(raw_folder, med_school_partials_folder, save=final_data_path):
    '''
    Pulls updates from the raw data folder and saves to a
    med school partial file. then concatenates all partial
    files to return one long database
    '''
    gpath = os.path.join(med_school_partials_folder,
                         'medical_schools_partial*.csv')
    df_list = []
    for pat in glob.glob(gpath):
        df_list.append(pd.read_csv(pat))

    df = pd.concat(df_list).drop(columns=['Unnamed: 0']).npi.drop_duplicates()

    [os.remove(x) for x in glob.glob(os.path.join(raw_folder, '*npi.txt'))]
    fi = sorted(list(set([os.path.basename(x).split('.txt')[0]
                          for x
                          in glob.glob(os.path.join(raw_folder, '*.txt'))])))
    fi = [x.split('_')[1] for x in fi]
    new_npis = (pd.DataFrame(dict(npi=fi))
                  .query('npi!=""')
                  .astype(int)
                  .drop_duplicates()
                  .merge(df, how='left', indicator=True)
                  .query('_merge=="left_only"'))
    print('new NPIs', len(new_npis.npi))
    df_long, not_found = retrieve_npis(new_npis.npi)
    if not df_long.empty:
        df_long['medical_school_upper'] = df_long.medical_school.str.upper()
        m = max([int(os.path.basename(x)
                       .split('.csv')[0]
                       .replace('medical_schools_partial', ''))
                 for x in glob.glob(gpath)]) + 1
        savepath = os.path.join(med_school_partials_folder,
                                'medical_schools_partial%s.csv' % m)
        cols = ['npi', 'medical_school_upper', 'grad_year']
        (df_long[cols].drop_duplicates().sort_values('npi').to_csv(savepath))
    df_list = []
    for pat in glob.glob(gpath):
        df_list.append(pd.read_csv(pat))
    updated = pd.concat(df_list).drop(columns=['Unnamed: 0']).drop_duplicates()
    if save:
        updated.to_csv(save, index=False)
    return updated, not_found


def sanitize_mds(fail_report=False):
    '''
    Note: this exercise uncovers the fact that there are some
    real MDs who are not using MD taxcodes.
    A more thorough fix would look at their credential string as well, and go
    back and make sure those were all crawled for schools as well
    Many of the below fails are actually podiatrists and chiropractors and
    others who apparently have schooling listed on their licenses. those
    are appropriate to throw out.
    '''
    schools = pd.read_csv(final_data_path)
    dups = schools.dropna()[schools.dropna().npi.duplicated()]
    schools = (schools[
        ~schools.npi.isin(dups.npi.drop_duplicates())].append(dups).dropna())
    schools.reset_index(drop=True, inplace=True)
    schools['grad_year'] = schools.grad_year.astype(int)
    assert schools.npi.is_unique

    # Real MDs:
    from ..utils.globalcache import c
    npi = NPI()
    taxcode = c.get_taxcode(npi.src, None, npi.entity, [1])
    mds = (taxcode.query('cat == "MD/DO" or cat == "MD/DO Student"')
                  .npi.drop_duplicates())
    schools2 = schools.merge(mds, how='right', indicator=True)
    matches = (schools2.query('_merge=="both"')
                       .drop(columns='_merge')
                       .assign(grad_year=lambda x: x.grad_year.astype(int)))
    not_found = schools2.query('_merge=="right_only"').npi.drop_duplicates()
    rept = (schools.merge(matches, how='left', indicator=True)
                   .query('_merge!="both"')
                   .medical_school_upper
                   .value_counts())
    if fail_report:
        longprint(rept)
    return matches, not_found
