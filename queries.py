import pandas as pd

other_md_codes = ['202C00000X', '202K00000X', '204C00000X',
                  '204D00000X', '204E00000X', '204F00000X',
                  '204R00000X', '209800000X']


def likely_md_do():
    df_tax = pd.read_csv('/work/akilby/npi/data/ptaxcode.csv')
    df_tax['prefix'] = df_tax.ptaxcode.str[:3]
    md_do_student = df_tax.query(
        'prefix=="207" or prefix=="208" or ptaxcode=="390200000X"')
    md_do_student = md_do_student[['npi', 'month']].drop_duplicates()

    md_do_student2 = df_tax.merge(pd.DataFrame({'ptaxcode', other_md_codes}))
    
    # only want individuals
    df_entity = pd.read_csv('/work/akilby/npi/data/entity.csv')
    md_do_student = md_do_student.merge(df_entity, how='left')

    df_pcredential = pd.read_csv('/work/akilby/npi/data/pcredential.csv')
    df_pcredentialoth = pd.read_csv('/work/akilby/npi/data/pcredentialoth.csv')



def medicaid_providers(state=None):

    df1 = pd.read_csv('/work/akilby/npi/data/OTHPID.csv')
    df2 = pd.read_csv('/work/akilby/npi/data/OTHPIDST.csv')
    df3 = pd.read_csv('/work/akilby/npi/data/OTHPIDTY.csv')

    df = df1.merge(df2, how='left').merge(df3, how='left')
    df[['npi', 'month', 'seq']].drop_duplicates()

    state_medicaid = df_all2.query('OTHPIDTY==5 and OTHPIDST=="IL"')
    df_entity = pd.read_csv('/work/akilby/npi/data/entity.csv')
    state_medicaid = state_medicaid.merge(df_entity, how='left')
