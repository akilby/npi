import calendar
import os
import urllib
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
