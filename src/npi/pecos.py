from .process.medicare import part_b_files, part_d_files
from .process.physician_compare import process_vars


def otherstuff():
    partd = part_d_files(summary=True, usecols=['npi', 'total_claim_count'])
    partb = part_b_files(summary=True,
                         columns=['National Provider Identifier',
                                  'Number of Medicare Beneficiaries'])
    claims = (partb.rename(columns={'National Provider Identifier': 'npi'})
                   .merge(partd, how='outer')
                   .sort_values(['npi', 'Year'])
                   .reset_index(drop=True))


    # ADD PHYSICIAN COMPARE
    cols = ['Medical school name', 'Graduation year',
            'Group Practice PAC ID', 'Number of Group Practice members']
    pc = process_vars(cols, drop_duplicates=False, date_var=True)
    grad_years = (pc.groupby(['NPI', 'Graduation year'])
                    .size()
                    .reset_index()
                    .sort_values(['NPI', 0])
                    .groupby('NPI')
                    .last()
                    .drop(columns=0)
                    .reset_index())
    pc = (pc[['NPI', 'date']].assign(Year=pc.date.dt.year)
                             .drop(columns='date')
                             .drop_duplicates()
                             .assign(physician_compare=1))
    medicare = (claims.merge(pc.rename(columns={'NPI': 'npi'}), how='outer')
                      .sort_values(['npi', 'Year'])
                      .merge(grad_years.rename(columns={'NPI': 'npi'}),
                             how='left', on='npi'))
    medicare['Graduation year'] = medicare['Graduation year'].astype('Int64')
    medicare = (medicare.merge(medicare[['npi', 'Year']].groupby('npi').max()
                                                        .reset_index()
                                                        .rename(
                                                            columns={'Year':
                                                                     'MaxYear'}
                                                                     )))

    "drop if old grad year, or if they drop out after high historical claims (if only a few claims, maybe cant determine)"
    "people can move to private practice..."
