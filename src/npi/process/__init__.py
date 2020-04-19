
PC_COLNAMES = [
               'NPI', 'PAC ID', 'Professional Enrollment ID', 'Last Name',
               'First Name', 'Middle Name', 'Suffix', 'Gender', 'Credential',
               'Medical school name', 'Graduation year', 'Primary specialty',
               'Secondary specialty 1', 'Secondary specialty 2',
               'Secondary specialty 3', 'Secondary specialty 4',
               'All secondary specialties', 'Organization legal name',
               'Group Practice PAC ID', 'Number of Group Practice members',
               'Line 1 Street Address', 'Line 2 Street Address',
               'Marker of address line 2 suppression', 'City', 'State',
               'Zip Code',
               'Hospital affiliation CCN 1', 'Hospital affiliation LBN 1',
               'Hospital affiliation CCN 2',  'Hospital affiliation LBN 2',
               'Hospital affiliation CCN 3',  'Hospital affiliation LBN 3',
               'Hospital affiliation CCN 4',  'Hospital affiliation LBN 4',
               'Hospital affiliation CCN 5',  'Hospital affiliation LBN 5',
               'Professional accepts Medicare Assignment',
               'Reported Quality Measures', 'Used electronic health records',
               'Committed to heart health through the Million Hearts'
               ' initiative'
               ]

PC_COL_DICT = {'NPI': 'NPI',
               'Ind_PAC_ID': 'PAC ID',
               'Ind_enrl_ID': 'Professional Enrollment ID',
               'lst_nm': 'Last Name',
               'frst_nm': 'First Name',
               'mid_nm': 'Middle Name',
               'suff': 'Suffix',
               'gndr': 'Gender',
               'Cred': 'Credential',
               'Med_sch': 'Medical school name',
               'Grd_yr': 'Graduation year',
               'Pri_spec': 'Primary specialty',
               'Sec_spec_1': 'Secondary specialty 1',
               'Sec_spec_2': 'Secondary specialty 2',
               'Sec_spec_3': 'Secondary specialty 3',
               'Sec_spec_4': 'Secondary specialty 4',
               'Sec_spec_all': 'All secondary specialties',
               'Org_lgl_nm': 'Organization legal name',
               'Org_PAC_ID': 'Group Practice PAC ID',
               'num_org_mem': 'Number of Group Practice members',
               'adr_ln_1': 'Line 1 Street Address',
               'adr_ln_2': 'Line 2 Street Address',
               'ln_2_sprs': 'Marker of address line 2 suppression',
               'cty': 'City',
               'st': 'State',
               'zip': 'Zip Code',
               'hosp_afl_1': 'Hospital affiliation CCN 1',
               'hosp_afl_lbn_1': 'Hospital affiliation LBN 1',
               'hosp_afl_2': 'Hospital affiliation CCN 2',
               'hosp_afl_lbn_2': 'Hospital affiliation LBN 2',
               'hosp_afl_3': 'Hospital affiliation CCN 3',
               'hosp_afl_lbn_3': 'Hospital affiliation LBN 3',
               'hosp_afl_4': 'Hospital affiliation CCN 4',
               'hosp_afl_lbn_4': 'Hospital affiliation LBN 4',
               'hosp_afl_5': 'Hospital affiliation CCN 5',
               'hosp_afl_lbn_5': 'Hospital affiliation LBN 5',
               'assgn': 'Professional accepts Medicare Assignment'}

PC_COL_DICT_REVERSE = {x: y for y, x in PC_COL_DICT.items()}

DTYPES = {'NPI': 'int',
          'Graduation year': 'Int64',
          'Medical school name': 'string',
          'Group Practice PAC ID': 'Int64',
          'Number of Group Practice members': 'Int64',
          'Primary specialty': 'string',
          'Secondary specialty 1': 'string',
          'Secondary specialty 2': 'string',
          'Secondary specialty 3': 'string',
          'Secondary specialty 4': 'string'
          }


PARTB_COLNAMES = {'National Provider Identifier ':
                  'National Provider Identifier',
                  'National Provider Identifier':
                  'National Provider Identifier',
                  'NPI': 'National Provider Identifier',
                  'Last Name/Organization Name':
                  'Last Name/Organization Name of the Provider',
                  'Last Name/Organization Name of the Provider':
                  'Last Name/Organization Name of the Provider',
                  'First Name': 'First Name of the Provider',
                  'First Name of the Provider': 'First Name of the Provider',
                  'Middle Initial': 'Middle Initial of the Provider',
                  'Middle Initial of the Provider':
                  'Middle Initial of the Provider',
                  'Credentials': 'Credentials of the Provider',
                  'Credentials of the Provider': 'Credentials of the Provider',
                  'Gender': 'Gender of the Provider',
                  'Gender of the Provider': 'Gender of the Provider',
                  'Entity Code': 'Entity Type of the Provider',
                  'Entity Type of the Provider': 'Entity Type of the Provider',
                  'Street Address 1': 'Street Address 1 of the Provider',
                  'Street Address 1 of the Provider':
                  'Street Address 1 of the Provider',
                  'Street Address 2': 'Street Address 2 of the Provider',
                  'Street Address 2 of the Provider':
                  'Street Address 2 of the Provider',
                  'City': 'City of the Provider',
                  'City of the Provider': 'City of the Provider',
                  'Zip Code': 'Zip Code of the Provider',
                  'Zip Code of the Provider': 'Zip Code of the Provider',
                  'State Code': 'State Code of the Provider',
                  'State Code of the Provider': 'State Code of the Provider',
                  'Country Code': 'Country Code of the Provider',
                  'Country Code of the Provider':
                  'Country Code of the Provider',
                  'Provider Type': 'Provider Type',
                  'Provider Type of the Provider': 'Provider Type',
                  'Medicare Participation': 'Medicare Participation Indicator',
                  'Medicare Participation Indicator':
                  'Medicare Participation Indicator',
                  'Place of Service': 'Place of Service',
                  'HCPCS_CODE': 'HCPCS Code',
                  'HCPCS Code': 'HCPCS Code',
                  'HCPCS_DESCRIPTION': 'HCPCS Description',
                  'HCPCS Description': 'HCPCS Description',
                  'HCPCS_DRUG_INDICATOR': 'HCPCS Drug Indicator',
                  'HCPCS Drug Indicator': 'HCPCS Drug Indicator',
                  'Identifies HCPCS As Drug Included in the ASP Drug List':
                  'HCPCS Drug Indicator',
                  'LINE_SRVC_CNT': 'Number of Services',
                  'Number of Services': 'Number of Services',
                  'BENE_UNIQUE_CNT': 'Number of Medicare Beneficiaries',
                  'Number of Medicare Beneficiaries':
                  'Number of Medicare Beneficiaries',
                  'BENE_DAY_SRVC_CNT':
                  'Number of Distinct Medicare Beneficiary/Per Day Services',
                  'Number of Distinct Medicare Beneficiary/Per Day Services':
                  'Number of Distinct Medicare Beneficiary/Per Day Services',
                  'Number of Medicare Beneficiary/Day Services':
                  'Number of Distinct Medicare Beneficiary/Per Day Services',
                  'AVERAGE_MEDICARE_ALLOWED_AMT':
                  'Average Medicare Allowed Amount',
                  'Average Medicare Allowed Amount':
                  'Average Medicare Allowed Amount',
                  'AVERAGE_SUBMITTED_CHRG_AMT':
                  'Average Submitted Charge Amount',
                  'Average Submitted Charge Amount':
                  'Average Submitted Charge Amount',
                  'STDEV_SUBMITTED_CHRG_AMT':
                  'Standard Deviation of Submitted Charge Amount',
                  'Standard Deviation of Submitted Charge Amount':
                  'Standard Deviation of Submitted Charge Amount',
                  'AVERAGE_MEDICARE_PAYMENT_AMT':
                  'Average Medicare Payment Amount',
                  'Average Medicare Payment Amount':
                  'Average Medicare Payment Amount',
                  'STDEV_MEDICARE_PAYMENT_AMT':
                  'Standard Deviation of Medicare Payment Amount',
                  'Standard Deviation of Medicare Payment Amount':
                  'Standard Deviation of Medicare Payment Amount',
                  'Average Medicare Standardized Amount':
                  'Average Medicare Standardized Amount',
                  'STDEV_MEDICARE_ALLOWED_AMT':
                  'Standard Deviation of Medicare Allowed Amount',
                  'Standard Deviation of Medicare Allowed Amount':
                  'Standard Deviation of Medicare Allowed Amount',
                  'NPPES Provider Last Name / Organization Name':
                  'Last Name/Organization Name of the Provider',
                  'NPPES Provider First Name': 'First Name of the Provider',
                  'NPPES Provider Middle Initial':
                  'Middle Initial of the Provider',
                  'NPPES Credentials': 'Credentials of the Provider',
                  'NPPES Provider Gender': 'Gender of the Provider',
                  'NPPES Entity Code': 'Entity Type of the Provider',
                  'NPPES Provider Street Address 1':
                  'Street Address 1 of the Provider',
                  'NPPES Provider Street Address 2':
                  'Street Address 2 of the Provider',
                  'NPPES Provider City': 'City of the Provider',
                  'NPPES Provider Zip Code': 'Zip Code of the Provider',
                  'NPPES Provider State': 'State Code of the Provider',
                  'NPPES Provider Country': 'Country Code of the Provider',
                  'Number of Unique Beneficiaries':
                  'Number of Medicare Beneficiaries',
                  'Total Submitted Charges': 'Total Submitted Charge Amount',
                  'Number of Unique Beneficiaries With Drug Services':
                  'Number of Medicare Beneficiaries With Drug Services',
                  'Total Drug Submitted Charges':
                  'Total Drug Submitted Charge Amount',
                  'Number of Unique Beneficiaries With Medical Services':
                  'Number of Medicare Beneficiaries With Medical Services',
                  'Total Medical Submitted Charges':
                  'Total Medical Submitted Charge Amount',
                  'Number of Beneficiaries Age Less than 65':
                  'Number of Beneficiaries Age Less 65'}
