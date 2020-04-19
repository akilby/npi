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


def singleton(list_or_set):
    assert len(list_or_set) == 1
    return list(list_or_set)[0]


def coerce_dtypes(col, orig_dtype, final_dtype):
    '''
    Converts to destination dtype and runs some sanity checks
    to make sure no harm has been done in the conversion
    '''
    new_col = col.astype(final_dtype)

    # Checks countable nulls are maintained
    assert new_col.isna().sum() == col.isna().sum()

    if final_dtype.lower().startswith('int'):
        # This checks numeric types are actually integers and I'm not
        # asserting/enforcing rounding
        assert (new_col.astype(final_dtype)
                       .astype(float)
                       .equals(new_col.astype(float)))

    assert all(new_col.index == col.index)
    return new_col


def convert_dtypes(df, dtypes):
    '''
    '''
    current_dtypes = {x: 'int' for x in df.select_dtypes('int').columns}
    for t in ['object', ['float32', 'float64'], 'datetime', 'string']:
        current_dtypes.update({x: t for x in df.select_dtypes(t).columns})
    for col in df.columns:
        final_dtype = dtypes[col]
        if (current_dtypes[col] != final_dtype and
                final_dtype not in current_dtypes[col]):
            try:
                df = df.assign(**{col: coerce_dtypes(df[col],
                                                     current_dtypes[col],
                                                     final_dtype)})
            except ValueError as err:
                if final_dtype == 'string':
                    newcol = coerce_dtypes(df[col], current_dtypes[col], 'str')
                    newcol = coerce_dtypes(newcol, 'str', 'string')
                else:
                    raise ValueError("{0}".format(err))
    return df


def force_integer(x):
    try:
        return int(x)
    except ValueError:
        return None


def force_integer_blanks(x, forcematch):
    try:
        return int(x)
    except ValueError:
        if x == forcematch:
            return None
        else:
            raise ValueError
