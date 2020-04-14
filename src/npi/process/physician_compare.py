import glob
import os

import pandas as pd

from ..constants import RAW_PC_DIR

colnames = ['NPI', 'PAC ID', 'Professional Enrollment ID', 'Last Name',
            'First Name', 'Middle Name', 'Suffix', 'Gender', 'Credential',
            'Medical school name', 'Graduation year', 'Primary specialty',
            'Secondary specialty 1', 'Secondary specialty 2',
            'Secondary specialty 3', 'Secondary specialty 4',
            'All secondary specialties', 'Organization legal name',
            'Group Practice PAC ID', 'Number of Group Practice members',
            'Line 1 Street Address', 'Line 2 Street Address',
            'Marker of address line 2 suppression', 'City', 'State',
            'Zip Code',
            'Hospital affiliation CCN 1', 'Hospital affiliation LBN 1',
            'Hospital affiliation CCN 2',  'Hospital affiliation LBN 2',
            'Hospital affiliation CCN 3',  'Hospital affiliation LBN 3',
            'Hospital affiliation CCN 4',  'Hospital affiliation LBN 4',
            'Hospital affiliation CCN 5',  'Hospital affiliation LBN 5',
            'Professional accepts Medicare Assignment',
            'Reported Quality Measures', 'Used electronic health records',
            'Committed to heart health through the Million Hearts?? initiative'
            ]

data_dict = {'NPI': 'NPI',
             'Ind_PAC_ID': 'PAC ID',
             'Ind_enrl_ID': 'Professional Enrollment ID',
             'lst_nm': 'Last Name',
             'frst_nm': 'First Name',
             'mid_nm': 'Middle Name',
             'suff': 'Suffix',
             'gndr': 'Gender',
             'Cred': 'Credential',
             'Med_sch': 'Medical school name',
             'Grd_yr': 'Graduation year',
             'Pri_spec': 'Primary specialty',
             'Sec_spec_1': 'Secondary specialty 1',
             'Sec_spec_2': 'Secondary specialty 2',
             'Sec_spec_3': 'Secondary specialty 3',
             'Sec_spec_4': 'Secondary specialty 4',
             'Sec_spec_all': 'All secondary specialties',
             'Org_lgl_nm': 'Organization legal name',
             'Org_PAC_ID': 'Group Practice PAC ID',
             'num_org_mem': 'Number of Group Practice members',
             'adr_ln_1': 'Line 1 Street Address',
             'adr_ln_2': 'Line 2 Street Address',
             'ln_2_sprs': 'Marker of address line 2 suppression',
             'cty': 'City',
             'st': 'State',
             'zip': 'Zip Code',
             'hosp_afl_1': 'Hospital affiliation CCN 1',
             'hosp_afl_lbn_1': 'Hospital affiliation LBN 1',
             'hosp_afl_2': 'Hospital affiliation CCN 2',
             'hosp_afl_lbn_2': 'Hospital affiliation LBN 2',
             'hosp_afl_3': 'Hospital affiliation CCN 3',
             'hosp_afl_lbn_3': 'Hospital affiliation LBN 3',
             'hosp_afl_4': 'Hospital affiliation CCN 4',
             'hosp_afl_lbn_4': 'Hospital affiliation LBN 4',
             'hosp_afl_5': 'Hospital affiliation CCN 5',
             'hosp_afl_lbn_5': 'Hospital affiliation LBN 5',
             'assgn': 'Professional accepts Medicare Assignment'}

data_dict_reverse = {x: y for y, x in data_dict.items()}

lf = ['Medical school name', 'Graduation year']


def expand_list_of_vars(lookfor):
    return ['NPI'] + [data_dict_reverse[x] for x in lookfor] + lookfor


def process_vars(lookfor, drop_duplicates=True, month_var=False):
    df_final = pd.DataFrame()
    for filename in glob.glob(os.path.join(RAW_PC_DIR, '*/*.csv')):
        try:
            if filename == os.path.join(RAW_PC_DIR,
                                        'Refresh_Data_Archive_07_2016/'
                                        'Refresh_Data_Archive_07_2016.csv'):
                df = pd.read_csv(filename, index_col=False,
                                 skiprows=[0], header=None)
                dropli = list((df.isnull().sum() == df.shape[0]).reset_index()[
                    df.isnull().sum() == df.shape[0]]['index'].values)
                df = df.drop(columns=dropli)
                df.columns = pd.read_csv(
                    filename, index_col=False, nrows=0).columns
            elif filename == os.path.join(RAW_PC_DIR,
                                          'Refresh_Data_Archive_December_2014/'
                                          'National_Downloadable_File.csv'):
                df = pd.read_csv(filename,  engine="python", sep=',',
                                 quotechar='"', error_bad_lines=False)
            elif (filename == os.path.join(RAW_PC_DIR,
                                           'Refresh_Data_Archive_June_2014/'
                                           'National_Downloadable_File.csv')
                  or filename == os.path.join(RAW_PC_DIR,
                                              'Refresh_Data_Archive_March_2014'
                                              '/National_Downloadable_File.csv'
                                              )):
                df = pd.read_csv(filename,  engine="python", sep=',',
                                 quotechar='"', error_bad_lines=False,
                                 header=None)
                df.columns = colnames
            else:
                df = pd.read_csv(
                    filename, index_col=False,
                    usecols=lambda x: str(x).strip()
                    in expand_list_of_vars(lookfor))
        except ValueError:
            print('FILENAME:', filename)
            raise Exception
        df.columns = [x.strip() for x in df.columns]
        df = df.rename(columns=data_dict)[['NPI'] + lookfor]
        df_final = df_final.append(df).drop_duplicates()
    return df_final
