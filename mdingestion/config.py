import pandas as pd
from os import path
import re

import logging


ETC_DIR = path.abspath(path.join(path.dirname(__file__), '..', '..', 'etc'))
IGNORE_LIST = path.join(ETC_DIR, 'ignore_urls.txt')
TO_IGNORE = None


def read_ignore_list():
    to_ignore = []
    try:
        df = pd.read_csv(
            IGNORE_LIST,
            delim_whitespace=True,
            comment='#',
            header=None,
            names=['url'])
        to_ignore = [re.compile(pattern) for pattern in list(df['url'][0:])]
    except Exception:
        logging.exception("Could not read ignore list")
    return to_ignore


def to_ignore():
    global TO_IGNORE

    if not TO_IGNORE:
        TO_IGNORE = read_ignore_list()
    return TO_IGNORE
