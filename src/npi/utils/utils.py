import calendar
import datetime
import glob
import os
import sys
import urllib
import urllib.request
import warnings
import zipfile
import zlib
from pprint import pprint
from urllib.request import urlopen

import pandas as pd
import requests
from tqdm import tqdm


def hasher(thing):
    return hash(thing) % ((sys.maxsize + 1) * 2)


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


def download_checkfirst(url, output_path, destructive=False):
    """
    Downloads a download path to output_path, checking first
    if that filename exists in that path

    Does not overwrite unless destructive=True
    """
    try:
        filename = detect_filename(url)
    except(urllib.error.HTTPError):
        print('Failed')
        return None
    if os.path.isdir(output_path):
        output_path = os.path.join(output_path, filename)

    if os.path.isfile(output_path) and not destructive:
        print('File already downloaded to: %s' % output_path)
    else:
        print('Downloading url: %s' % url)
        try:
            download_url(url, output_path)
        except(urllib.error.HTTPError):
            print('Failed')
            return None
    return output_path


def detect_filename(url):
    response = urlopen(url)
    return os.path.basename(response.url)


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def download_url(url, output_path):
    with DownloadProgressBar(unit='B', unit_scale=True,
                             miniters=1, desc=url.split('/')[-1]) as t:
        urllib.request.urlretrieve(url,
                                   filename=output_path,
                                   reporthook=t.update_to)


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
    try:
        new_col = col.astype(final_dtype)
    except ValueError:
        if final_dtype == 'string':
            new_col = (col.astype('str').astype('string')
                          .apply(lambda x: None if x == 'nan' else x)
                          .astype('string'))
        else:
            raise ValueError

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


def isid(df, index_cols, noisily=False, assertion=True):
    print_string = (f'Index columns {index_cols}%s'
                    ' uniquely identify the observations')
    if df.set_index(index_cols).index.is_unique:
        if noisily:
            print(print_string % '')
    else:
        if assertion:
            raise AssertionError(print_string % ' do not')
        else:
            print(print_string % ' do not')


def col_reorderer(df_cols, cols, how='first'):
    assert how in ['first', 'last']
    othercols = [x for x in df_cols if x not in cols]
    if how == 'first':
        return cols + othercols
    else:
        return othercols + cols


def stata_elapsed_date_to_datetime(date, fmt):
    """
    Original source for this code:
    https://www.statsmodels.org/0.8.0/_modules/statsmodels/iolib/foreign.html

    Convert from SIF to datetime. http://www.stata.com/help.cgi?datetime

    Parameters
    ----------
    date : int
        The Stata Internal Format date to convert to datetime according to fmt
    fmt : str
        The format to convert to. Can be, tc, td, tw, tm, tq, th, ty

    Examples
    --------
    >>> _stata_elapsed_date_to_datetime(52, "%tw")
    datetime.datetime(1961, 1, 1, 0, 0)

    Notes
    -----
    datetime/c - tc
        milliseconds since 01jan1960 00:00:00.000, assuming 86,400 s/day
    datetime/C - tC - NOT IMPLEMENTED
        milliseconds since 01jan1960 00:00:00.000, adjusted for leap seconds
    date - td
        days since 01jan1960 (01jan1960 = 0)
    weekly date - tw
        weeks since 1960w1
        This assumes 52 weeks in a year, then adds 7 * remainder of the weeks.
        The datetime value is the start of the week in terms of days in the
        year, not ISO calendar weeks.
    monthly date - tm
        months since 1960m1
    quarterly date - tq
        quarters since 1960q1
    half-yearly date - th
        half-years since 1960h1 yearly
    date - ty
        years since 0000

    If you don't have pandas with datetime support, then you can't do
    milliseconds accurately.
    """
    # NOTE: we could run into overflow / loss of precision situations here
    # casting to int, but I'm not sure what to do. datetime won't deal with
    # numpy types and numpy datetime isn't mature enough / we can't rely on
    # pandas version > 0.7.1
    # TODO: IIRC relative delta doesn't play well with np.datetime?
    date = int(date)
    stata_epoch = datetime.datetime(1960, 1, 1)
    if fmt in ["%tc", "tc"]:
        from dateutil.relativedelta import relativedelta
        return stata_epoch + relativedelta(microseconds=date*1000)
    elif fmt in ["%tC", "tC"]:
        from warnings import warn
        warn("Encountered %tC format. Leaving in Stata Internal Format.",
             UserWarning)
        return date
    elif fmt in ["%td", "td"]:
        return stata_epoch + datetime.timedelta(int(date))
    elif fmt in ["%tw", "tw"]:
        # does not count leap days - 7 days is a week
        year = datetime.datetime(stata_epoch.year + date // 52, 1, 1)
        day_delta = (date % 52) * 7
        return year + datetime.timedelta(int(day_delta))
    elif fmt in ["%tm", "tm"]:
        year = stata_epoch.year + date // 12
        month_delta = (date % 12) + 1
        return datetime.datetime(year, month_delta, 1)
    elif fmt in ["%tq", "tq"]:
        year = stata_epoch.year + date // 4
        month_delta = (date % 4) * 3 + 1
        return datetime.datetime(year, month_delta, 1)
    elif fmt in ["%th", "th"]:
        year = stata_epoch.year + date // 2
        month_delta = (date % 2) * 6 + 1
        return datetime.datetime(year, month_delta, 1)
    elif fmt in ["%ty", "ty"]:
        if date > 0:
            return datetime.datetime(date, 1, 1)
        else:
            # don't do negative years bc can't mix dtypes in column
            raise ValueError("Year 0 and before not implemented")
    else:
        raise ValueError("Date fmt %s not understood" % fmt)
