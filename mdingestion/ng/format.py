from dateutil import parser as date_parser
from shapely.geometry import shape
import re
from urllib.parse import urlparse

from .util import remove_duplicates_from_list

import logging


def format_value(value, type=None, one=False):
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
    elif type == 'date':
        formatted = format_date(text)
    elif type == 'date_year':
        formatted = format_date_year(text)
    elif type == 'string_words':
        formatted = format_string_words(text)
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


def format_date(text):
    try:
        parsed = date_parser.parse(text)
        val = parsed.isoformat().split('T')[0]
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
    parsed = urlparse(text)
    if not parsed.scheme:
        logging.warning(f"could not parse url: {text}")
        val = ''
    else:
        val = format_string(text)
    return val
