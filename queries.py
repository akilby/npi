import pandas as pd

other_md_codes = ['202C00000X', '202K00000X', '204C00000X',
                  '204D00000X', '204E00000X', '204F00000X',
                  '204R00000X', '209800000X']


def likely_doctors():
    df_tax = pd.read_csv('/work/akilby/npi/data/ptaxcode.csv')
    df_tax['prefix'] = df_tax.ptaxcode.str[:3]
    md_do_student = df_tax.query(
        'prefix=="207" or prefix=="208" or ptaxcode=="390200000X"')
    md_do_student = md_do_student[['npi', 'month']].drop_duplicates()

    md_do_student2 = df_tax.merge(pd.DataFrame({'ptaxcode': other_md_codes}))
    md_do_student2 = md_do_student2[['npi', 'month']].drop_duplicates()

    md_do_student = md_do_student.append(md_do_student2)
    
    # only want individuals
    df_entity = pd.read_csv('/work/akilby/npi/data/entity.csv')
    md_do_student = md_do_student.merge(df_entity, how='left')
    md_do_student = md_do_student.query("entity==1").drop(columns=['entity'])

    # Look in credentials
    df_pcredential = pd.read_csv('/work/akilby/npi/data/pcredential.csv')
    df_pcredential['stripped'] = (df_pcredential.pcredential
                                                .str.replace('.', '')
                                                .str.replace(' ', '')
                                                .str.strip()
                                                .str.upper())
    md_do = df_pcredential.query('stripped=="MD" or stripped=="DO"')
    
    df_pcredentialoth = pd.read_csv('/work/akilby/npi/data/pcredentialoth.csv')
    df_pcredentialoth['stripped'] = (df_pcredentialoth.dropna()
                                                      .pcredentialoth
                                                      .str.replace('.', '')
                                                      .str.replace(' ', '')
                                                      .str.strip()
                                                      .str.upper())
    md_do2 = df_pcredentialoth.query('stripped=="MD" or stripped=="DO"')

    cred = pd.concat([md_do[['npi']].drop_duplicates(),
                      md_do2[['npi']].drop_duplicates()])

    md_all = (pd.concat([cred, md_do_student[['npi']].drop_duplicates()])
                .drop_duplicates())
    return md_all


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
