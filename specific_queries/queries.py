import pandas as pd
from NPI_Clean import NPI, src


def likely_doctors():
    npi = NPI(src=src)
    npi.retrieve('credentials')
    npi.retrieve('taxcode')
    npi.credentials['stripped'] = (npi.credentials.pcredential
                                                  .str.replace('.', '')
                                                  .str.replace(' ', '')
                                                  .str.strip()
                                                  .str.upper())
    md_do = npi.credentials.query('stripped=="MD" or stripped=="DO"')

    doctors = (pd.concat(
        [md_do.npi,
         npi.taxcode.query('cat=="MD/DO" or cat=="MD/DO Student"').npi])
                 .drop_duplicates())
    return doctors


def medicaid_providers(state=None,
                       usecols=['entity',
                                'pfname', 'pfnameoth',
                                'plname', 'plnameoth',
                                'pcredential', 'pgender',
                                'porgname',
                                'plocline1', 'plocline2', 'ploccityname',
                                'plocstatename', 'ploczip', 'ploctel'],
                       usecols_wide=['ptaxcode', 'PLICNUM', 'PLICSTATE']):

    df1 = pd.read_csv('/work/akilby/npi/data/OTHPID.csv')
    df2 = pd.read_csv('/work/akilby/npi/data/OTHPIDST.csv')
    df3 = pd.read_csv('/work/akilby/npi/data/OTHPIDTY.csv')

    df = df1.merge(df2, how='left').merge(df3, how='left')
    assert df[['npi', 'month', 'seq']].drop_duplicates().shape[0] == df.shape[0]

    q = ('OTHPIDTY==5 and OTHPIDST=="%s"' % state) if state else 'OTHPIDTY==5'
    medicaid_npis = df.query(q).npi.drop_duplicates()
    othpid = df.merge(medicaid_npis)

    list_of_dfs = []
    for col in usecols:
        print(col)
        dfrc = pd.DataFrame()
        dfr = pd.read_csv('/work/akilby/npi/data/%s.csv' % col, iterator=True,
                          chunksize=1000000)
        for dfrs in dfr:
            dfrc = dfrc.append(dfrs[dfrs.npi.isin(medicaid_npis)])

        list_of_dfs.append(dfrc)

    usecols_dfs = list_of_dfs[0]
    icol = ['npi', 'month']
    new_columns = icol + (usecols_dfs.columns.drop(icol).tolist())
    usecols_dfs = usecols_dfs[new_columns]
    for d in list_of_dfs[1:]:
        usecols_dfs = usecols_dfs.merge(d, how='outer')

    df_lic = pd.DataFrame()
    dfr = pd.read_csv('/work/akilby/npi/data/%s.csv' % 'PLICNUM', iterator=True,
                      chunksize=1000000)
    for dfrs in dfr:
        df_lic = df_lic.append(dfrs[dfrs.npi.isin(medicaid_npis)])

    df_lics = pd.DataFrame()
    dfr = pd.read_csv('/work/akilby/npi/data/%s.csv' % 'PLICSTATE', iterator=True,
                      chunksize=1000000)
    for dfrs in dfr:
        df_lics = df_lics.append(dfrs[dfrs.npi.isin(medicaid_npis)])
    
    dfl = df_lics.merge(df_lic, how='outer')

    df_tax = pd.DataFrame()
    dfr = pd.read_csv('/work/akilby/npi/data/%s.csv' % 'ptaxcode', iterator=True,
                      chunksize=1000000)
    for dfrs in dfr:
        df_tax = df_tax.append(dfrs[dfrs.npi.isin(medicaid_npis)])

    return othpid, usecols_dfs, dfl, df_tax
