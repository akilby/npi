import random

from googleapiclient.discovery import build
from npi.npi import NPI, src
from project_management.helper import pickle_dump

npi = NPI(src, entities=1)
npi.retrieve('removaldate')
npi.retrieve('ptaxcode')
npi.retrieve('fullnames')

search_df = (npi.removaldate
                .merge(npi.ptaxcode
                          .query('cat=="MD/DO"')
                          .npi.drop_duplicates())
                .sort_values('npideactdate'))

__author__ = 'a.kilby@northeastern.edu'
service = build("customsearch",
                "v1",
                developerKey="AIzaSyBsag8cCRT9CDXEQrl7vgZCDLNhE1nq4CU")

npi_list = search_df.npi.tolist()

npi_list = [x for x in npi_list if x != 1467775023 and x != 1487674206]


########
# if can't find a hit at first, go to the expanded fullnames, search the full
# web for name + "obituary"


def store_obit_results(use_npi, npi):
    removal = (npi.removaldate
                  .query('npi==%s' % use_npi)[['npideactdate']]
                  .astype(str).npideactdate.values[0])

    name = npi.fullnames.query('npi==%s' % use_npi)
    if name.pmname.values[0] != '':
        use_name = (name.pfname.values[0] + ' ' + name.pmname.values[0]
                    + ' ' + name.plname.values[0])
    else:
        use_name = name.pfname.values[0] + ' ' + name.plname.values[0]

    # can remove siterestrict()
    res = service.cse().siterestrict().list(
            q=use_name,
            cx='012963847452809474986:wr7atzbkzti'
        ).execute()

    pickle_dump(res, '/work/akilby/npi/raw_obit/%s.pkl' % use_npi)

    print(use_npi, use_name, removal)
    if int(res['searchInformation']['totalResults']) > 0:
        for i in range(len(res['items'])):
            print(i)
            print(res['items'][i]['title'])
            print(res['items'][i]['snippet'])
            if 'pagemap' in res['items'][i].keys():
                d = res['items'][i]['pagemap']['metatags'][0]
                if 'pdate' in d.keys():
                    print(d['pdate'])
                if 'pubdate' in d.keys():
                    print(d['pubdate'])


for i in range(898):
    random.shuffle(npi_list)
    use_npi, npi_list = npi_list[:1][0], npi_list[1:]
    store_obit_results(use_npi, npi)
