import calendar
from pprint import pprint

import pandas as pd
import requests


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
