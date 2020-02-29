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

    if state:
        medicaid = df.query('OTHPIDTY==5 and OTHPIDST=="%s"' % state)
    else:
        medicaid = df.query('OTHPIDTY==5')
    for col in usecols:
        dfm = pd.read_csv('/work/akilby/npi/data/%s.csv' % col)
        medicaid = medicaid.merge(dfm, how='left')
    
    medicaid_short = medicaid[['npi', 'month']].drop_duplicates()
    
    df_lic = pd.read_csv('/work/akilby/npi/data/%s.csv' % 'PLICNUM')
    df_lics = pd.read_csv('/work/akilby/npi/data/%s.csv' % 'PLICSTATE')
    dfl = df_lics.merge(df_lic, how='outer')
    medicaid_lic = medicaid_short.merge(dfl)
    dft = pd.read_csv('/work/akilby/npi/data/ptaxcode.csv')

    medicaid_tax = medicaid_short.merge(dft)
    
    return medicaid, medicaid_tax, medicaid_lic
