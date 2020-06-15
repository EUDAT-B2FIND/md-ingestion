import pandas as pd

import logging


def parse_source_list(filename):
    sources = dict()
    if not filename:
        return sources
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
        logging.warning(f"Could not read source list {filename}")
    return sources


def remove_duplicates_from_list(a_list):
    return list(dict.fromkeys(a_list))
