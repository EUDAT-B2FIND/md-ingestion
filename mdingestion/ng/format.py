from dateutil import parser as date_parser
from shapely.geometry import shape
import re
from urllib.parse import urlparse

from .util import remove_duplicates_from_list

import logging


def format_value(value, type=None, one=False, min_length=None, max_length=None):
    # work with value list
    values = value or []
    if not isinstance(values, list):
        values = [values]
    # format values to type
    formatted = [format(val, type) for val in values]
    # drop empty values
    formatted = [val for val in formatted if val]
    # remove duplicates
    formatted = remove_duplicates_from_list(formatted)
    if min_length:
        formatted = [val for val in formatted if len(val) >= min_length]
    if max_length:
        formatted = [val[:max_length] for val in formatted]
    # do we have a single value?
    if one:
        if formatted:
            result = formatted[0]
        else:
            result = ''
    else:
        result = formatted
    return result


def format(text, type=None):
    type = type or 'string'
    if type == 'string':
        formatted = format_string(text)
    elif type == 'float':
        formatted = format_float(text)
    elif type == 'bool' or type == 'boolean':
        formatted = format_bool(text)
    elif type == 'datetime':
        formatted = format_datetime(text)
    elif type == 'date':
        formatted = format_date(text)
    elif type == 'date_year':
        formatted = format_date_year(text)
    elif type == 'string_words':
        formatted = format_string_words(text)
    elif type == 'string_word':
        formatted = format_string_word(text)
    elif type == 'url':
        formatted = format_url(text)
    else:
        formatted = format_string(text)
    return formatted


def format_string(text):
    if text is None:
        value = ''
    else:
        value = f'{text}'.strip()
    return value


def format_string_words(text):
    if text is None:
        value = ''
    else:
        value = ' '.join(re.findall(r'\w+', text.strip()))
    return value


def format_string_word(text):
    return format_string_words(text).split(' ')[0]


def format_float(text):
    try:
        val = float(text)
    except Exception:
        logging.warning(f"could not parse float: {text}")
        val = float("nan")  # nan = not a number
    return val


def format_bool(text):
    try:
        val = bool(text)
    except Exception:
        logging.warning(f"could not parse boolean: {text}")
        val = ''
    return val


def format_datetime(text):
    try:
        parsed = date_parser.parse(text)
        val = parsed.isoformat(timespec='seconds')
        val = f"{val}Z"
    except Exception:
        logging.warning(f"could not parse datetime: {text}")
        val = ''
    return val


def format_date(text):
    try:
        val = format_datetime(text).split('T')[0]
    except Exception:
        logging.warning(f"could not parse date: {text}")
        val = ''
    return val


def format_date_year(text):
    try:
        val = format_date(text).split('-')[0]
    except Exception:
        logging.warning(f"could not parse date_year: {text}")
        val = ''
    return val


def format_url(text):
    url = format_string(text)
    parsed = urlparse(url)
    if parsed.scheme in ['http', 'https', 'ftp']:
        pass
    elif parsed.scheme == 'urn':
        url = resolve_urn(text)
    # TODO: fix herbadrop ark
    elif parsed.scheme in ['ark', ]:
        pass
    elif parsed.scheme == 'doi' or parsed.path.startswith('10.'):
        url = f"https://doi.org/{parsed.path}"
    else:
        logging.warning(f"could not parse url: {url}")
        url = ''
    return url


def resolve_urn(urn):
    if urn.startswith('urn:nbn'):
        url = f'https://nbn-resolving.org/{urn}'
    else:
        url = ''
    return url
