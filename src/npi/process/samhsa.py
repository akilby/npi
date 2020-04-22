
import re

import pandas as pd

from ..npi import expand_names_in_sensible_ways

source_file = ('/work/akilby/npi/samhsa_processing/'
               'FOIA_12312019_datefilled_clean_NPITelefill.csv')


class SAMHSA(object):
    def __init__(self, src=source_file):
        self.source_file = src
        self.samhsa = self.make_samhsa_id(pd.read_csv(src, low_memory=False))
        self.get_names()

    def make_samhsa_id(self, samhsa, idvars=['NameFull', 'DateLastCertified',
                                             'PractitionerType']):
        """
        SAMHSA files do not have an identifier. Make an arbitrary one for
        tracking throughout the class. Note: this will not be stable across
        different versions of the SAMHSA data
        """
        for idvar in idvars:
            samhsa[idvar] = samhsa[idvar].str.upper()
        ids = (samhsa[idvars].drop_duplicates()
                             .reset_index(drop=True)
                             .reset_index()
                             .rename(columns=dict(index='samhsa_id')))
        return samhsa.merge(ids)

    def get_names(self, namecol='NameFull'):
        names = (self.samhsa[[namecol, 'samhsa_id']]
                     .drop_duplicates()
                     .sort_values('samhsa_id')
                     .reset_index(drop=True))
        # remove credentials from end
        badl = credential_suffixes()
        names = remove_suffixes(names, badl)
        names[namecol] = remove_mi_periods(names[namecol])
        while not remove_suffixes(names.copy(), badl).equals(names.copy()):
            names = remove_suffixes(names, badl)

        # split into first, middle, last -- note that a maiden name is middle
        # name here, could maybe use that later

        names_long = (names[namecol].str.replace('    ', ' ')
                                    .str.replace('   ', ' ')
                                    .str.replace('  ', ' ')
                                    .str.split(' ', expand=True)
                                    .stack()
                                    .reset_index())

        firstnames = names_long.groupby('level_0').first().reset_index()
        lastnames = names_long.groupby('level_0').last().reset_index()
        middlenames = (names_long.merge(firstnames, how='left', indicator=True)
                                 .query('_merge=="left_only"')
                                 .drop(columns='_merge')
                                 .merge(lastnames, how='left', indicator=True)
                                 .query('_merge=="left_only"')
                                 .drop(columns='_merge')
                                 .set_index(['level_0', 'level_1']).unstack(1))
        middlenames = middlenames.fillna('').agg(' '.join, axis=1).str.strip()

        allnames = (firstnames.merge(pd.DataFrame(middlenames),
                                     left_index=True, right_index=True,
                                     how='outer')
                              .merge(lastnames,
                                     left_index=True, right_index=True)
                              .fillna('')
                              .drop(columns=['level_1_x', 'level_1_y'])
                              .rename(columns={'0_x': 'firstname', '0_y':
                                               'middlename', 0: 'lastname'}))
        names = pd.concat([allnames, names], axis=1)

        cols = ['firstname', 'middlename', 'lastname', 'samhsa_id']
        newnames = expand_names_in_sensible_ways(
            names[cols].reset_index(drop=True),
            'samhsa_id', 'firstname', 'middlename', 'lastname')
        newnames = (newnames.append(names[cols + ['NameFull']].rename(
                                columns={'NameFull': 'name'}))
                            .drop_duplicates())

        names = newnames.merge(names[['samhsa_id', 'Credential String']])
        names = names.drop(columns='Credential String')
        names = (names.sort_values(['samhsa_id', 'middlename'],
                                   ascending=False)
                      .reset_index(drop=True))
        names = (
            names.merge(names.assign(order=1).groupby('samhsa_id').cumsum(),
                        left_index=True, right_index=True))
        # ## ADD SUFFIX AND TREAT LIKE A MIDDLE NAME IN TERMS OF ORDER
        self.names = (names.sort_values(['samhsa_id', 'order'])
                           .reset_index(drop=True))


def credential_suffixes():
    badl = ['(JR.)', '(M.D.)', '(RET.)', 'M., PH', 'M. D.', 'M.D.,',
            'M.D., PHD, DABA,',
            'M .D.', 'M.D.', 'MD', 'NP', 'D.O.', 'PA', 'DO', '.D.', 'MPH',
            'PH.D.', 'JR', 'M.D', 'PHD', 'P.A.', 'FNP', 'PA-C', 'M.P.H.',
            'N.P.', 'III', 'SR.', 'D.', '.D', 'FASAM', 'JR.', 'MS',
            'D', 'FAAFP', 'SR', 'D.O', 'CNS', 'F.A.S.A.M.', 'FNP-BC', 'P.C.',
            'MBA', 'M.S.', 'PH.D', 'FACP', 'M.P.H', 'CNM',
            'NP-C', 'MR.', 'MDIV', 'FACEP', 'PLLC', 'M.A.', 'LLC', 'MR',
            'DNP', 'PHD.', 'FNP-C', 'MD.', 'CNP', 'J.D.', 'IV', 'F.A.P.A.',
            'DR.', 'M.D,', 'DABPM', 'M,D.', 'MS.', 'FACOOG', 'APRN']

    badl2 = (pd.read_csv('/work/akilby/npi/samhsa_processing/stubs_rev.csv')
               .rename(columns={'Unnamed: 2': 'flag'})
               .query('flag==1')['Unnamed: 0']
               .tolist())

    badl = badl + badl2
    return badl


def remove_suffixes(samhsa, badl):
    name_col = samhsa.columns.tolist()[0]
    for b in badl:
        samhsa.loc[samhsa[name_col].apply(lambda x: x.endswith(' ' + b)),
                   'Credential String'] = (samhsa[name_col].apply(
                    lambda x: x[len(x)-len(b):]))
        samhsa[name_col] = (samhsa[name_col].apply(
            lambda x: x[:len(x)-len(b)] if x.endswith(' ' + b) else x))
        samhsa[name_col] = samhsa[name_col].str.strip()
        samhsa[name_col] = (samhsa[name_col].apply(
            lambda x: x[:-1] if x.endswith(',') else x))
    return samhsa


def remove_mi_periods(col):
    return col.apply(lambda x: re.sub('(.*\s[A-Z]?)(\.)(\s)', r'\1\3', x))
