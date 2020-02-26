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
    'plname': 'Provider Last Name (Legal Name)',
    'penumdate': 'Provider Enumeration Date',
    'orgname': 'Provider Organization Name (Legal Business Name)',
    'orgnameoth': 'Provider Other Organization Name',
    'orgnameothcode': 'Provider Other Organization Name Type Code',
    'fnameoth': 'Provider Other First Name',
    'licnum': ['Provider License Number_1',
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
    'licstate': ['Provider License Number State Code_1',
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
              'Provider License Number State Code_15']
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
    'plocline1': 'string',
    'plocline2': 'string',
    'ploccityname': 'string',
    'plocstatename': 'string',
    'ploczip': 'string',
    'pcredential': 'string',
    'pcredentialoth': 'string',
    'penumdate': 'datetime64[ns]',
    'pfname': 'string',
    'plname': 'string',
    'pgender': 'string',
    'porgname': 'string',
    'ploctel': 'int',
    'pfnameoth': 'string',
    'plnameoth': 'string',
    'plicnum': 'string',
    'plicstate': 'string',
    'replacement_npi': 'int',
    'soleprop': 'string'
}
