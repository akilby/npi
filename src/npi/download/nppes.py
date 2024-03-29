"""
Code written collaboratively with @amin.p and @akilby

Downloads all NBER npi data for years in yearlist and months in monthslist
and variables in USE_VAR_LIST

Downloads all master dissemination files from the NBER and CMS

Downloads the weekly updates
"""

import calendar
import os
import urllib.error
from pprint import pprint

import pandas as pd
import requests
import wget

from ..constants import DISSEM_PATHS, NBER_PATH, RAW_DATA_DIR, USE_VAR_LIST
from ..utils.utils import unzip_checkfirst


def nppes_month_list():
    """
    NPI/NPPES data is available from Nov 2007 to present
    """
    cyear = pd.to_datetime('today').year
    cmonth = pd.to_datetime('today').month
    return [(x, y) for x in range(2007, cyear + 1)
            for y in range(1, 13)
            if not (x == 2007 and y < 11)
            and not (x == cyear and y > cmonth)]


def nppes_weekly_update_list():
    """
    Lists the weekly zip files now on the server
    """
    cms = 'https://download.cms.gov/nppes'
    return [cms +
            x.split('href=".')[1].split('.zip')[0] + '.zip'
            for x in
            requests.get(cms + '/NPI_Files.html').text.splitlines()
            if '_Weekly.zip' in x]


def wget_checkfirst(url, to_dir, nondestructive=True):
    """
    Wgets a download path to to_dir, checking first
    if that filename exists in that path

    Does not overwrite unless nondestructive=False
    """
    filename = wget.detect_filename(url)
    destination_path = os.path.join(to_dir, filename)
    if os.path.isfile(destination_path) and nondestructive:
        print('File already downloaded to: %s' % destination_path)
    else:
        print('WGetting url: %s' % url)
        try:
            wget.download(url, out=to_dir)
        except(urllib.error.HTTPError):
            print('Failed')
            return False
    return True


def wget_nber(year, month, variable):
    """
    Downloads a year, month, variable single-variable file
    from NBER NPPES host
    """
    stub = os.path.join(NBER_PATH, str(year), str(month),
                        '%s%s%s' % (variable, year, month))
    if not wget_checkfirst('%s.csv' % stub, to_dir=RAW_DATA_DIR):
        if not wget_checkfirst('%s.dta' % stub, to_dir=RAW_DATA_DIR):
            return False
    return True


def process_fail_list(download_fail_list):
    """
    Processes download results from NBER single column files,
    and returns a list of months that are
    substantially incomplete (less than 5 variables observed)
    """
    results = (pd.DataFrame([(key[0], key[1], key[2], val)
                            for key, val in download_fail_list.items()])
               .set_index([0, 1, 2])
               .unstack())
    results.columns = [x[1] for x in list(results.columns)]
    results['total'] = results.sum(axis=1)
    incomplete_months = list(results.query("total<5").index)
    return results, incomplete_months


def dissem_file_potential_paths(year, month):
    """
    generates a list of potential urls at which we might find thes
    NPI zipped dissemination files. Sources include nber and cms.
    """
    p = []
    ndd = 'NPPES_Data_Dissemination'
    mname = calendar.month_name[month]
    mabbr = calendar.month_abbr[month]
    DPATHS = [d % year if '%s' in d else d for d in DISSEM_PATHS]
    for dpath in DPATHS:
        p.append(os.path.join(dpath, '%s_%s_%s.zip' % (ndd, mname, year)))
        p.append(os.path.join(dpath, '%s_%s_%s.zip' % (ndd, mabbr, year)))
    return p


def wget_data_dissemination_zips(year, month, to_dir):
    """
    """
    found, i = False, 0
    paths = dissem_file_potential_paths(year, month)
    while not found and i < len(paths):
        path = paths[i]
        found = wget_checkfirst(path, to_dir=to_dir)
        i += 1
    path_return = path if found else None
    return found, path_return


def main():
    # Ensure destination folder exists
    if not os.path.isdir(RAW_DATA_DIR):
        os.makedirs(RAW_DATA_DIR)

    # Single-variable files from NBER
    params = [(x, y, z) for z in USE_VAR_LIST for x, y in nppes_month_list()]

    # Old: download from NBER
    # result_list = {x: wget_nber(*x) for x in params}

    # New: just list what is downloaded, since NBER has broken
    result_list = {x: True if f'{x[2]}{x[0]}{x[1]}.csv'
                   in os.listdir('/work/akilby/npi/raw/')
                   else False for x in params}

    # Process failed downloads
    results, missing_months = process_fail_list(result_list)

    # Download large data dissemination files
    result_list = {x: wget_data_dissemination_zips(*list(x) + [RAW_DATA_DIR])
                   for x in missing_months}
    pprint([key for key, val in result_list.items() if not val[0]])

    # Download weekly updates
    weeklies = {x: wget_checkfirst(x, RAW_DATA_DIR)
                for x in nppes_weekly_update_list()}

    # Unzip large data dissemination files
    zipfiles1 = [os.path.join(RAW_DATA_DIR, wget.detect_filename(val[1]))
                 for key, val in result_list.items() if val[0]]
    zipfiles2 = [os.path.join(RAW_DATA_DIR, wget.detect_filename(key))
                 for key, val in weeklies.items() if val]
    zipfiles = zipfiles1 + zipfiles2
    [unzip_checkfirst(z, os.path.splitext(z)[0]) for z in zipfiles]


if __name__ == '__main__':
    main()
