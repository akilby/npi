RAW_DATA_PATH = '/work/akilby/npi/raw'

USE_VAR_LIST = ['taxcode', 'locline1', 'locline2',
                'loccityname', 'locstatename', 'loczip',
                'orgname', 'loctel', 'orgnameothcode',
                'credential', 'credentialoth', 'fname',
                'fnameoth', 'gender', 'lname', 'licnum',
                'licstate', 'primtax', 'enumdate']

# Add variable for is this NPI for an organization or a person

USE_VAR_LIST_DICT = {
    'taxcode': ['Healthcare Provider Taxonomy Code_1',
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
    'locline1': 'Provider First Line Business Practice Location Address',
    'locline2': 'Provider Second Line Business Practice Location Address',
    'loccityname': 'Provider Business Practice Location Address City Name',
    'locstatename': 'Provider Business Practice Location Address State Name',
    'loczip': 'Provider Business Practice Location Address Postal Code',
    'loctel': 'Provider Business Practice Location Address Telephone Number',
    'credential': 'Provider Credential Text',
    'credentialoth':  'Provider Other Credential Text',
    'fname': 'Provider First Name',
    'gender': 'Provider Gender Code',
    'lname': 'Provider Last Name (Legal Name)'
}

DTYPES = {
    'npi': 'int',
    'seq': 'int',
    'ptaxcode': 'string',
    'pprimtax': 'string',
    'ptaxgroup': 'string',
}
