import sys
sys.path.append('/home/akilby/Packages/npi')

from queries import medicaid_providers

print('starting')
othpid, usecols_dfs, dfl, df_tax = medicaid_providers(state="IL")
othpid.to_csv('/work/akilby/npi/processed/medicaid_IL_othpid.csv')
usecols_dfs.to_csv('/work/akilby/npi/processed/medicaid_IL_pinfo.csv')
dfl.to_csv('/work/akilby/npi/processed/medicaid_IL_lic.csv')
df_tax.to_csv('/work/akilby/npi/processed/medicaid_IL_tax.csv')
