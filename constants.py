NBER_PATH = 'https://data.nber.org/npi/byvar'
RAW_DATA_DIR = '/work/akilby/npi/raw'
DATA_DIR = '/work/akilby/npi/data'

DISSEM_PATHS = ['http://download.cms.gov/nppes/',
                'https://data.nber.org/npi/%s/',
                'https://data.nber.org/npi/backfiles/']

USE_MONTH_LIST = ['Jan', 'Feb', 'March', 'April', 'May', 'June',
                  'Jul', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec']

USE_VAR_LIST = ['entity',
                'pfname',
                'pmname',
                'plname',
                'pfnameoth',
                'pmnameoth',
                'plnameoth',
                'plnamecode',
                'penumdate',
                'pgender',
                'pcredential',
                'pcredentialoth',
                'plocline1',
                'plocline2',
                'ploccityname',
                'plocstatename',
                'ploczip',
                'ploctel',
                'plicnum',
                'PLICSTATE',
                'porgname',
                'porgnameoth',
                'porgnameothcode',
                'ptaxcode',
                'replacement_npi',
                'soleprop',
                'lastupdate']


USE_VAR_LIST_DICT = {
    'npi': 'NPI',
    'entity': 'Entity Type Code',
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
    'plocline1': 'Provider First Line Business Practice Location Address',
    'plocline2': 'Provider Second Line Business Practice Location Address',
    'ploccityname': 'Provider Business Practice Location Address City Name',
    'plocstatename': 'Provider Business Practice Location Address State Name',
    'ploczip': 'Provider Business Practice Location Address Postal Code',
    'ploctel': 'Provider Business Practice Location Address Telephone Number',
    'pfname': 'Provider First Name',
    'pgender': 'Provider Gender Code',
    'plname': 'Provider Last Name (Legal Name)'
}

l1 = {val: key for key, val in USE_VAR_LIST_DICT.items()
      if not isinstance(val, list)}
l2 = {x: 'ptaxcode' for x in [val for key, val in USE_VAR_LIST_DICT.items()
      if isinstance(val, list)][0]}

USE_VAR_LIST_DICT_REVERSE = {**l1, **l2}

# Integer types are int, which is numpy and cannot contain
# null values, and Int64, which is
# a new Pandas type that can hold nan values

DTYPES = {
    'npi': 'int',
    'entity': "Int64",
    'seq': 'int',
    'ptaxcode': 'string',
    'pprimtax': 'string',
    'ptaxgroup': 'string',
}
