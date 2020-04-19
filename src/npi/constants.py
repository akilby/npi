import os

NBER_PATH = 'https://data.nber.org/npi/byvar'
RAW_DATA_DIR = '/work/akilby/npi/raw'
DATA_DIR = '/work/akilby/npi/data'

RAW_PC_DIR = '/work/akilby/npi/raw_medicare/physician_compare'
PC_UPDATE_URL = ('https://data.medicare.gov/api/views/mj5m-pzi6/'
                 'rows.csv?accessType=DOWNLOAD&sorting=true')

DISSEM_PATHS = ['http://download.cms.gov/nppes/',
                'https://data.nber.org/npi/%s/',
                'https://data.nber.org/npi/backfiles/']

USE_MONTH_LIST = ['Jan', 'Feb', 'March', 'April', 'May', 'June',
                  'Jul', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec']

API_SOURCE_PATH = ('https://data.cms.gov/'
                   'api/views/'
                   '%s/rows.csv?accessType=DOWNLOAD')

PART_D_SOURCE_PATH = ('http://download.cms.gov/'
                      'Research-Statistics-Data-and-Systems/'
                      'Statistics-Trends-and-Reports/'
                      'Medicare-Provider-Charge-Data/'
                      'Downloads/')

PART_B_RAW_DIR = '/work/akilby/npi/raw_medicare/medicare_part_b'

PART_D_RAW_DIR = '/work/akilby/npi/raw_medicare/medicare_part_d'

PART_D_OPI_RAW_DIR = '/work/akilby/npi/raw_medicare/medicare_part_d_opioids'

PART_B_STUB_SUM = ('Medicare_Physician_and_Other_Supplier_National_Provider'
                   '_Identifier__NPI__Aggregate_Report__Calendar_Year_')

PART_B_STUB = ('Medicare_Provider_Utilization_and_Payment_Data__Physician'
               '_and_Other_Supplier_')

PART_D_OPI_STUB = 'Medicare_Part_D_Opioid_Prescriber_Summary_File_'

USE_VAR_LIST_DICT = {
    'npi': 'NPI',
    'entity': 'Entity Type Code',
    'lastupdate': 'Last Update Date',
    'pcredential': 'Provider Credential Text',
    'pcredentialoth':  'Provider Other Credential Text',
    'ptaxcode': ['Healthcare Provider Taxonomy Code_1',
                 'Healthcare Provider Taxonomy Code_2',
                 'Healthcare Provider Taxonomy Code_3',
                 'Healthcare Provider Taxonomy Code_4',
                 'Healthcare Provider Taxonomy Code_5',
                 'Healthcare Provider Taxonomy Code_6',
                 'Healthcare Provider Taxonomy Code_7',
                 'Healthcare Provider Taxonomy Code_8',
                 'Healthcare Provider Taxonomy Code_9',
                 'Healthcare Provider Taxonomy Code_10',
                 'Healthcare Provider Taxonomy Code_11',
                 'Healthcare Provider Taxonomy Code_12',
                 'Healthcare Provider Taxonomy Code_13',
                 'Healthcare Provider Taxonomy Code_14',
                 'Healthcare Provider Taxonomy Code_15'],
    'PTAXGROUP': ['Healthcare Provider Taxonomy Group_1',
                  'Healthcare Provider Taxonomy Group_2',
                  'Healthcare Provider Taxonomy Group_3',
                  'Healthcare Provider Taxonomy Group_4',
                  'Healthcare Provider Taxonomy Group_5',
                  'Healthcare Provider Taxonomy Group_6',
                  'Healthcare Provider Taxonomy Group_7',
                  'Healthcare Provider Taxonomy Group_8',
                  'Healthcare Provider Taxonomy Group_9',
                  'Healthcare Provider Taxonomy Group_10',
                  'Healthcare Provider Taxonomy Group_11',
                  'Healthcare Provider Taxonomy Group_12',
                  'Healthcare Provider Taxonomy Group_13',
                  'Healthcare Provider Taxonomy Group_14',
                  'Healthcare Provider Taxonomy Group_15'],
    'PPRIMTAX': ['Healthcare Provider Primary Taxonomy Switch_1',
                 'Healthcare Provider Primary Taxonomy Switch_2',
                 'Healthcare Provider Primary Taxonomy Switch_3',
                 'Healthcare Provider Primary Taxonomy Switch_4',
                 'Healthcare Provider Primary Taxonomy Switch_5',
                 'Healthcare Provider Primary Taxonomy Switch_6',
                 'Healthcare Provider Primary Taxonomy Switch_7',
                 'Healthcare Provider Primary Taxonomy Switch_8',
                 'Healthcare Provider Primary Taxonomy Switch_9',
                 'Healthcare Provider Primary Taxonomy Switch_10',
                 'Healthcare Provider Primary Taxonomy Switch_11',
                 'Healthcare Provider Primary Taxonomy Switch_12',
                 'Healthcare Provider Primary Taxonomy Switch_13',
                 'Healthcare Provider Primary Taxonomy Switch_14',
                 'Healthcare Provider Primary Taxonomy Switch_15'],
    'plocline1': 'Provider First Line Business Practice Location Address',
    'plocline2': 'Provider Second Line Business Practice Location Address',
    'ploccityname': 'Provider Business Practice Location Address City Name',
    'plocstatename': 'Provider Business Practice Location Address State Name',
    'ploczip': 'Provider Business Practice Location Address Postal Code',
    'ploctel': 'Provider Business Practice Location Address Telephone Number',
    'ploc2line1':
        'Provider Secondary Practice Location Address- Address Line 1',
    'ploc2line2':
        'Provider Secondary Practice Location Address-  Address Line 2',
    'ploc2cityname':
        'Provider Secondary Practice Location Address - City Name',
    'ploc2statename':
        'Provider Secondary Practice Location Address - State Name',
    'ploc2zip': 'Provider Secondary Practice Location Address - Postal Code',
    'ploc2tel':
        'Provider Secondary Practice Location Address - Telephone Number',
    'pfname': 'Provider First Name',
    'pmname': 'Provider Middle Name',
    'pgender': 'Provider Gender Code',
    'plname': 'Provider Last Name (Legal Name)',
    'plnameoth': 'Provider Other Last Name',
    'pmnameoth': 'Provider Other Middle Name',
    'penumdate': 'Provider Enumeration Date',
    'porgname': 'Provider Organization Name (Legal Business Name)',
    'porgnameoth': 'Provider Other Organization Name',
    'porgnameothcode': 'Provider Other Organization Name Type Code',
    'pfnameoth': 'Provider Other First Name',
    'npideactdate': 'NPI Deactivation Date',
    'npireactdate': 'NPI Reactivation Date',
    'orgsubpart': 'Is Organization Subpart',
    'parent_org_lbn': 'Parent Organization LBN',
    'PLICNUM': ['Provider License Number_1',
                'Provider License Number_2',
                'Provider License Number_3',
                'Provider License Number_4',
                'Provider License Number_5',
                'Provider License Number_6',
                'Provider License Number_7',
                'Provider License Number_8',
                'Provider License Number_9',
                'Provider License Number_10',
                'Provider License Number_11',
                'Provider License Number_12',
                'Provider License Number_13',
                'Provider License Number_14',
                'Provider License Number_15'],
    'PLICSTATE': ['Provider License Number State Code_1',
                  'Provider License Number State Code_2',
                  'Provider License Number State Code_3',
                  'Provider License Number State Code_4',
                  'Provider License Number State Code_5',
                  'Provider License Number State Code_6',
                  'Provider License Number State Code_7',
                  'Provider License Number State Code_8',
                  'Provider License Number State Code_9',
                  'Provider License Number State Code_10',
                  'Provider License Number State Code_11',
                  'Provider License Number State Code_12',
                  'Provider License Number State Code_13',
                  'Provider License Number State Code_14',
                  'Provider License Number State Code_15'],
    'soleprop': 'Is Sole Proprietor',
    'replacement_npi': 'Replacement NPI',
    'OTHPID': ['Other Provider Identifier_1',
               'Other Provider Identifier_2',
               'Other Provider Identifier_3',
               'Other Provider Identifier_4',
               'Other Provider Identifier_5',
               'Other Provider Identifier_6',
               'Other Provider Identifier_7',
               'Other Provider Identifier_8',
               'Other Provider Identifier_9',
               'Other Provider Identifier_10',
               'Other Provider Identifier_11',
               'Other Provider Identifier_12',
               'Other Provider Identifier_13',
               'Other Provider Identifier_14',
               'Other Provider Identifier_15',
               'Other Provider Identifier_16',
               'Other Provider Identifier_17',
               'Other Provider Identifier_18',
               'Other Provider Identifier_19',
               'Other Provider Identifier_20',
               'Other Provider Identifier_21',
               'Other Provider Identifier_22',
               'Other Provider Identifier_23',
               'Other Provider Identifier_24',
               'Other Provider Identifier_25',
               'Other Provider Identifier_26',
               'Other Provider Identifier_27',
               'Other Provider Identifier_28',
               'Other Provider Identifier_29',
               'Other Provider Identifier_30',
               'Other Provider Identifier_31',
               'Other Provider Identifier_32',
               'Other Provider Identifier_33',
               'Other Provider Identifier_34',
               'Other Provider Identifier_35',
               'Other Provider Identifier_36',
               'Other Provider Identifier_37',
               'Other Provider Identifier_38',
               'Other Provider Identifier_39',
               'Other Provider Identifier_40',
               'Other Provider Identifier_41',
               'Other Provider Identifier_42',
               'Other Provider Identifier_43',
               'Other Provider Identifier_44',
               'Other Provider Identifier_45',
               'Other Provider Identifier_46',
               'Other Provider Identifier_47',
               'Other Provider Identifier_48',
               'Other Provider Identifier_49',
               'Other Provider Identifier_50'],
    'OTHPIDISS': ['Other Provider Identifier Issuer_1',
                  'Other Provider Identifier Issuer_2',
                  'Other Provider Identifier Issuer_3',
                  'Other Provider Identifier Issuer_4',
                  'Other Provider Identifier Issuer_5',
                  'Other Provider Identifier Issuer_6',
                  'Other Provider Identifier Issuer_7',
                  'Other Provider Identifier Issuer_8',
                  'Other Provider Identifier Issuer_9',
                  'Other Provider Identifier Issuer_10',
                  'Other Provider Identifier Issuer_11',
                  'Other Provider Identifier Issuer_12',
                  'Other Provider Identifier Issuer_13',
                  'Other Provider Identifier Issuer_14',
                  'Other Provider Identifier Issuer_15',
                  'Other Provider Identifier Issuer_16',
                  'Other Provider Identifier Issuer_17',
                  'Other Provider Identifier Issuer_18',
                  'Other Provider Identifier Issuer_19',
                  'Other Provider Identifier Issuer_20',
                  'Other Provider Identifier Issuer_21',
                  'Other Provider Identifier Issuer_22',
                  'Other Provider Identifier Issuer_23',
                  'Other Provider Identifier Issuer_24',
                  'Other Provider Identifier Issuer_25',
                  'Other Provider Identifier Issuer_26',
                  'Other Provider Identifier Issuer_27',
                  'Other Provider Identifier Issuer_28',
                  'Other Provider Identifier Issuer_29',
                  'Other Provider Identifier Issuer_30',
                  'Other Provider Identifier Issuer_31',
                  'Other Provider Identifier Issuer_32',
                  'Other Provider Identifier Issuer_33',
                  'Other Provider Identifier Issuer_34',
                  'Other Provider Identifier Issuer_35',
                  'Other Provider Identifier Issuer_36',
                  'Other Provider Identifier Issuer_37',
                  'Other Provider Identifier Issuer_38',
                  'Other Provider Identifier Issuer_39',
                  'Other Provider Identifier Issuer_40',
                  'Other Provider Identifier Issuer_41',
                  'Other Provider Identifier Issuer_42',
                  'Other Provider Identifier Issuer_43',
                  'Other Provider Identifier Issuer_44',
                  'Other Provider Identifier Issuer_45',
                  'Other Provider Identifier Issuer_46',
                  'Other Provider Identifier Issuer_47',
                  'Other Provider Identifier Issuer_48',
                  'Other Provider Identifier Issuer_49',
                  'Other Provider Identifier Issuer_50'],
    'OTHPIDST': ['Other Provider Identifier State_1',
                 'Other Provider Identifier State_2',
                 'Other Provider Identifier State_3',
                 'Other Provider Identifier State_4',
                 'Other Provider Identifier State_5',
                 'Other Provider Identifier State_6',
                 'Other Provider Identifier State_7',
                 'Other Provider Identifier State_8',
                 'Other Provider Identifier State_9',
                 'Other Provider Identifier State_10',
                 'Other Provider Identifier State_11',
                 'Other Provider Identifier State_12',
                 'Other Provider Identifier State_13',
                 'Other Provider Identifier State_14',
                 'Other Provider Identifier State_15',
                 'Other Provider Identifier State_16',
                 'Other Provider Identifier State_17',
                 'Other Provider Identifier State_18',
                 'Other Provider Identifier State_19',
                 'Other Provider Identifier State_20',
                 'Other Provider Identifier State_21',
                 'Other Provider Identifier State_22',
                 'Other Provider Identifier State_23',
                 'Other Provider Identifier State_24',
                 'Other Provider Identifier State_25',
                 'Other Provider Identifier State_26',
                 'Other Provider Identifier State_27',
                 'Other Provider Identifier State_28',
                 'Other Provider Identifier State_29',
                 'Other Provider Identifier State_30',
                 'Other Provider Identifier State_31',
                 'Other Provider Identifier State_32',
                 'Other Provider Identifier State_33',
                 'Other Provider Identifier State_34',
                 'Other Provider Identifier State_35',
                 'Other Provider Identifier State_36',
                 'Other Provider Identifier State_37',
                 'Other Provider Identifier State_38',
                 'Other Provider Identifier State_39',
                 'Other Provider Identifier State_40',
                 'Other Provider Identifier State_41',
                 'Other Provider Identifier State_42',
                 'Other Provider Identifier State_43',
                 'Other Provider Identifier State_44',
                 'Other Provider Identifier State_45',
                 'Other Provider Identifier State_46',
                 'Other Provider Identifier State_47',
                 'Other Provider Identifier State_48',
                 'Other Provider Identifier State_49',
                 'Other Provider Identifier State_50'],
    'OTHPIDTY': ['Other Provider Identifier Type Code_1',
                 'Other Provider Identifier Type Code_2',
                 'Other Provider Identifier Type Code_3',
                 'Other Provider Identifier Type Code_4',
                 'Other Provider Identifier Type Code_5',
                 'Other Provider Identifier Type Code_6',
                 'Other Provider Identifier Type Code_7',
                 'Other Provider Identifier Type Code_8',
                 'Other Provider Identifier Type Code_9',
                 'Other Provider Identifier Type Code_10',
                 'Other Provider Identifier Type Code_11',
                 'Other Provider Identifier Type Code_12',
                 'Other Provider Identifier Type Code_13',
                 'Other Provider Identifier Type Code_14',
                 'Other Provider Identifier Type Code_15',
                 'Other Provider Identifier Type Code_16',
                 'Other Provider Identifier Type Code_17',
                 'Other Provider Identifier Type Code_18',
                 'Other Provider Identifier Type Code_19',
                 'Other Provider Identifier Type Code_20',
                 'Other Provider Identifier Type Code_21',
                 'Other Provider Identifier Type Code_22',
                 'Other Provider Identifier Type Code_23',
                 'Other Provider Identifier Type Code_24',
                 'Other Provider Identifier Type Code_25',
                 'Other Provider Identifier Type Code_26',
                 'Other Provider Identifier Type Code_27',
                 'Other Provider Identifier Type Code_28',
                 'Other Provider Identifier Type Code_29',
                 'Other Provider Identifier Type Code_30',
                 'Other Provider Identifier Type Code_31',
                 'Other Provider Identifier Type Code_32',
                 'Other Provider Identifier Type Code_33',
                 'Other Provider Identifier Type Code_34',
                 'Other Provider Identifier Type Code_35',
                 'Other Provider Identifier Type Code_36',
                 'Other Provider Identifier Type Code_37',
                 'Other Provider Identifier Type Code_38',
                 'Other Provider Identifier Type Code_39',
                 'Other Provider Identifier Type Code_40',
                 'Other Provider Identifier Type Code_41',
                 'Other Provider Identifier Type Code_42',
                 'Other Provider Identifier Type Code_43',
                 'Other Provider Identifier Type Code_44',
                 'Other Provider Identifier Type Code_45',
                 'Other Provider Identifier Type Code_46',
                 'Other Provider Identifier Type Code_47',
                 'Other Provider Identifier Type Code_48',
                 'Other Provider Identifier Type Code_49',
                 'Other Provider Identifier Type Code_50']
}


l1 = {val: key for key, val in USE_VAR_LIST_DICT.items()
      if not isinstance(val, list)}
# l2 = {x: 'ptaxcode' for x in [val for key, val in USE_VAR_LIST_DICT.items()
#       if isinstance(val, list)][0]}

li = [(val, key) for key, val in USE_VAR_LIST_DICT.items()
      if isinstance(val, list)]
list_of_dicts = [{key: val for key in lis} for lis, val in li]
l2 = {k: v for d in list_of_dicts for k, v in d.items()}

USE_VAR_LIST_DICT_REVERSE = {**l1, **l2}

# Integer types are int, which is numpy and cannot contain
# null values, and Int64, which is
# a new Pandas type that can hold nan values

DTYPES = {
    'npi': 'int',
    'entity': "Int64",
    'seq': 'int',
    'lastupdate': 'datetime64[ns]',
    'ptaxcode': 'string',
    'PPRIMTAX': 'string',
    'PTAXGROUP': 'string',
    'plocline1': 'string',
    'plocline2': 'string',
    'ploccityname': 'string',
    'plocstatename': 'string',
    'ploczip': 'string',
    'ploc2line1': 'string',
    'ploc2line2': 'string',
    'ploc2cityname': 'string',
    'ploc2statename': 'string',
    'ploc2zip': 'string',
    'ploc2tel': 'string',
    'pcredential': 'string',
    'pcredentialoth': 'string',
    'porgnameothcode': 'Int64',
    'porgnameoth': 'string',
    'penumdate': 'datetime64[ns]',
    'pfname': 'string',
    'pmname': 'string',
    'plname': 'string',
    'pgender': 'string',
    'porgname': 'string',
    'ploctel': 'string',
    'pfnameoth': 'string',
    'pmnameoth': 'string',
    'plnameoth': 'string',
    'PLICNUM': 'string',
    'PLICSTATE': 'string',
    'replacement_npi': 'Int64',
    'soleprop': 'string',
    'OTHPID': 'string',
    'OTHPIDISS': 'string',
    'OTHPIDST': 'string',
    'OTHPIDTY': "Int64",
    'npideactdate': 'datetime64[ns]',
    'npireactdate': 'datetime64[ns]',
    'orgsubpart': 'string',
    'parent_org_lbn': 'string',
}

USE_VAR_LIST = [x for x in
                list(set(list(USE_VAR_LIST_DICT.keys()) + list(DTYPES.keys())))
                if (x in USE_VAR_LIST_DICT.keys()
                    and x in DTYPES.keys()
                    and x != 'npi')]

VARS_NOT_MADE = [x for x in USE_VAR_LIST
                 if '%s.csv' % x not in os.listdir(DATA_DIR)]
