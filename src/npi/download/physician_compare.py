"""
https://data.cms.gov/provider-data/topics/doctors-clinicians

These are the official datasets used on Medicare.gov provided by the Centers
for Medicare & Medicaid Services. These datasets give you useful information
about doctors, clinicians, and groups listed on Medicare Care Compare.

General information about doctors and clinicians in the Provider Data Catalog
and on Medicare Care Compare profile pages comes primarily from the Provider,
Enrollment, Chain, and Ownership System (PECOS) and is checked against Medicare
claims data. This information is updated twice a month.

For a clinician or group's information to appear on Care Compare, they must
have:

Current and “approved” Medicare enrollment records in PECOS
A valid physical practice location or address
A valid specialty
A National Provider Identifier (NPI) for a clinician
At least one Medicare Fee-for-Service claim within the last six months for a
    clinician
At least two approved clinicians reassigning their benefits to the group


https://data.cms.gov/provider-data/archived-data/doctors-clinicians

https://wayback.archive-it.org/org-551/20160104131342/https://data.medicare.gov/data/archives/physician-compare

The Medicare Fee-For-Service Public Provider Enrollment (PPEF) dataset includes
information on providers who are actively approved to bill Medicare or have
completed the 855O at the time the data was pulled from the Provider Enrollment
and Chain Ownership System (PECOS). The release of this provider enrollment
data is not related to other provider information releases such as Physician
Compare or Data Transparency.

https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/medicare-fee-for-service-public-provider-enrollment

https://data.cms.gov/resources/fee-for-service-public-provider-enrollment-methodology

https://www.nber.org/research/data/medicare-fee-service-public-provider-enrollment-data

You’re required to revalidate—or renew—your enrollment record periodically to
maintain Medicare billing privileges. In general, providers and suppliers
revalidate every five years but DMEPOS suppliers revalidate every three years.
CMS also reserves the right to request off-cycle revalidations.

https://www.cms.gov/Medicare/Provider-Enrollment-and-Certification/Revalidations

The Provider Enrollment data will be published on
https://data.cms.gov/public-provider-enrollment and will be updated on a
quarterly basis. The initial data will consist of individual and organization
provider and supplier enrollment information similar to what is on Physician
Compare; however, it will be directly from PECOS and will only be updated
through updates to enrollment information.

https://www.cms.gov/newsroom/fact-sheets/public-provider-and-supplier-enrollment-files

"""

import os
import re

import pandas as pd
import requests

from ..constants import PC_UPDATE_URL, RAW_PC_DIR
from ..utils.utils import unzip_checkfirst, wget_checkfirst


def physician_compare_get_date_updated():
    t = requests.get('https://data.medicare.gov/data/physician-compare').text
    d = re.findall(
        'data was last updated on\n\s+[A-Za-z]+\s+[0-9]+,\s+[0-9]+', t)
    return pd.to_datetime(' '.join(d[0].split()[5:])).isoformat().split('T')[0]


def physician_compare_archive_list():
    stub = 'https://data.medicare.gov/data/archives/physician-compare'
    t = requests.get(stub).text
    return [x.split('<a href="')[1] for x in
            re.findall('<a href="[A-Za-z./0-9_:]+', t) if 'medicare.gov' in x]


def get_physician_compare_update():
    """
    Note: this renames the natural filename,
    Physician_Compare_National_Downloadable_File.csv,
    to one including the update date.
    """
    ud = physician_compare_get_date_updated()
    to_dir = os.path.join(RAW_PC_DIR, 'Updates')
    name1 = 'Physician_Compare_National_Downloadable_File%s%s.csv' % ('_', ud)
    name2 = 'Physician_Compare_National_Downloadable_File%s%s.csv' % ('', '')
    update_path = os.path.join(to_dir, name1)
    if not os.path.isfile(update_path):
        wget_checkfirst(PC_UPDATE_URL, to_dir)
        os.rename(os.path.join(to_dir, name2), update_path)
    else:
        print('Already up-to-date')
    return update_path


def main():

    # 1. archived data on CMS
    urls = physician_compare_archive_list()
    zipfiles = [wget_checkfirst(u, RAW_PC_DIR) for u in urls]
    [unzip_checkfirst(z, os.path.splitext(z)[0]) for z in zipfiles]

    # 2. check if there is new updated data on CMS
    get_physician_compare_update()

    # 3. note that some archived files were downloaded from the NBER,
    # https://data.nber.org/compare/physician/ .
    # Those files needed to be extracted on my PC before being uploaded.
    # They are now in the NBER folder, and others exist on the NBER
    # server that I did not download.


if __name__ == "__main__":
    main()
