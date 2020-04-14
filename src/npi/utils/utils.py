import calendar
import glob
import os
import urllib
import warnings
import zipfile
import zlib
from pprint import pprint

import pandas as pd
import requests
import wget


def longprint(df):
    '''Prints out a full dataframe'''
    with pd.option_context('display.max_rows',
                           None,
                           'display.max_columns',
                           None):  # more options can be specified also
        print(df)


def urlread(url):
    '''Prints all the text at specified URL'''
    response = requests.get(url)
    pprint(response.text)


def month_name_to_month_num(abb):
    try:
        return list(calendar.month_abbr).index(abb)
    except ValueError:
        return list(calendar.month_name).index(abb)


def wget_checkfirst(url, to_dir, destructive=False):
    """
    Wgets a download path to to_dir, checking first
    if that filename exists in that path

    Does not overwrite unless destructive=True
    """
    filename = wget.detect_filename(url)
    destination_path = os.path.join(to_dir, filename)
    if os.path.isfile(destination_path) and not destructive:
        print('File already downloaded to: %s' % destination_path)
    else:
        print('WGetting url: %s' % url)
        try:
            wget.download(url, out=to_dir)
        except(urllib.error.HTTPError):
            print('Failed')
            return None
    return destination_path


def unzip(path, to_dir):
    """
    """
    print('Unzipping File %s' % path, end=' ')
    try:
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(to_dir)
            print('... Unzipped')
    except(zlib.error):
        warnings.warn('Unzipping %s failed' % path)


def unzip_checkfirst_check(path, to_dir):
    '''
    Returns false if it looks like it is already unzipped
    '''
    if os.path.isdir(to_dir):
        s1 = os.path.getsize(path)
        s2 = sum([os.path.getsize(path) for path
                  in glob.glob(os.path.join(to_dir, '*'))])
    else:
        s1, s2 = 100, 0
    return False if s2 >= s1 else True


def unzip_checkfirst(path, to_dir, destructive=False):
    if unzip_checkfirst_check(path, to_dir) or destructive:
        unzip(path, to_dir)
    else:
        print('Path %s already appears unzipped to %s' % (path, to_dir))
