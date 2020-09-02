import pandas as pd

from .exceptions import CommunityNotFound

import logging

SOURCES = None


def read_harvest_list(filename):
    sources = dict()
    try:
        df = pd.read_csv(
            filename,
            delim_whitespace=True,
            comment='#',
            header=None,
            names=['community', 'url', 'verb', 'mdprefix', 'mdsubset'])
        df['mdsubset'] = df['mdsubset'].fillna('')
        for row_dict in df.to_dict(orient='records'):
            sources[row_dict['community']] = row_dict
    except Exception:
        logging.warning(f"Could not read harvest list {filename}")
    return sources


def get_source(filename, community):
    global SOURCES

    if not SOURCES:
        SOURCES = read_harvest_list(filename)
    if community not in SOURCES:
        raise CommunityNotFound
    return SOURCES[community]
