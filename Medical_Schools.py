import os
import random
import time

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


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

        # Safeguard on Column Titles
        # if len(column_names) > 0 and len(column_names) != n_columns:
        #     raise Exception("Column titles do not match"
        #                     "the number of columns")

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
    df_long = []
    not_found = []
    hp = HTMLTableParser()
    for npi in npi_list:
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
        if table:
            df1 = npi_data_scraped(npi, table, 'npino')
            df_long.append(df1)
        if table2:
            df2 = npi_data_scraped(npi, table2, 'npiprofile')
            df_long.append(df2)
        if not table and not table2:
            print('NPI %s: not found' % npi)
            not_found.append(npi)
    return pd.concat(df_long), not_found


# import sys
# sys.path.append('/home/akilby/Packages/npi/')
# from Medical_Schools import HTMLTableParser

# npi = 1710906169

# hp = HTMLTableParser()
# table = hp.parse_url('https://npino.com/npi/%s' % npi,
#                      save_path='/work/akilby/npi/raw_web/npino_%s.txt' % npi)
# 
# 



# def return_npi_info(npi):
#     r = random.randint(0, 1)
#     if r == 0:
#         out = (requests.get('https://npino.com/npi/%s' % npi)
#                        .text.split('Specialization')[1].split('Gender')[0])
#     else:
#         out = requests.get('https://npiprofile.com/npi/%s' % npi).text.split('<td>Gender</td><td>')[1].split('<td>Is Sole Proprietor?</td><td>')[0]
