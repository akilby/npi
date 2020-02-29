import sys
sys.path.append('/home/akilby/Packages/npi')

from queries import medicaid_providers

print('starting')
medicaid, medicaid_tax, medicaid_lic = medicaid_providers(state="IL")
medicaid.to_csv('/work/akilby/npi/processed/medicaid_IL.csv')
medicaid_tax.to_csv('/work/akilby/npi/processed/medicaid_IL_tax.csv')
medicaid_lic.to_csv('/work/akilby/npi/processed/medicaid_IL_lic.csv')
