"""
Ideas: if can't find a hit at first, go to the expanded fullnames, search the
full web for name + "obituary"
"""

import glob
import random

from googleapiclient.discovery import build
from npi.npi import NPI
from project_management.helper import pickle_dump

__author__ = 'a.kilby@northeastern.edu'
service = build("customsearch",
                "v1",
                developerKey="AIzaSyBsag8cCRT9CDXEQrl7vgZCDLNhE1nq4CU")


def npi_obj():
    npi = NPI(entities=1)
    npi.retrieve('removaldate')
    npi.retrieve('ptaxcode')
    npi.retrieve('fullnames')
    return npi


def npi_crawl_list(npi):
    search_df = (npi.removaldate
                    .merge(npi.ptaxcode
                              .query('cat=="MD/DO"')
                              .npi.drop_duplicates())
                    .sort_values('npideactdate'))

    npi_list = search_df.npi.tolist()

    already_retrieved = [int(x.split('/raw_obit/')[1].split('.pkl')[0])
                         for x in glob.glob('/work/akilby/npi/raw_obit/*.pkl')]

    npi_list = [x for x in npi_list if x not in already_retrieved]
    return npi_list


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


def crawl(num, npi, npi_crawl_list):
    for i in range(num):
        random.shuffle(npi_crawl_list)
        use_npi, npi_crawl_list = npi_crawl_list[:1][0], npi_crawl_list[1:]
        store_obit_results(use_npi, npi)
