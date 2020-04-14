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
