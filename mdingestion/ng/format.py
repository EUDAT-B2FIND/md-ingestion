from dateutil import parser as date_parser
from shapely.geometry import shape
import re
from urllib.parse import urlparse
import iso639

from .util import (
    remove_duplicates_from_list,
    is_valid_url,
    is_valid_email
)

import logging


NULL_VALUES = (
    '',
    'n/a',
    'none',
    'not stated',
    'not available',
)


def is_null_value(text):
    if isinstance(text, bool):
        return False
    if isinstance(text, float):
        return False
    if isinstance(text, int):
        return False
    if not text:
        return True
    if f"{text}".strip().lower() in NULL_VALUES:
        return True
    return False


def format_value(value, type=None, one=False, min_length=None, max_length=None):
    # work with value list
    values = value or []
    if not isinstance(values, list):
        values = [values]
    formatted = values
    # format values to type
    formatted = [format(val, type) for val in formatted]
    # drop empty values
    formatted = [val for val in formatted if not is_null_value(val)]
    # formatted = [val for val in formatted if val]
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
    elif type == 'language':
        formatted = format_language(text)
    elif type == 'email':
        formatted = format_email(text)
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


def format_language(text):
    try:
        # TODO: use https://pypi.org/project/pycountry/
        val = iso639.to_name(format_string_word(text))
    except Exception:
        logging.warning(f"could not match language: {text}")
        val = ''
    return val


def format_email(text):
    email = format_string(text)
    if email and is_valid_email(email):
        email = email.replace('@', '(at)')
    # TODO: quick fix for email in "contact" (string)
    if '@' in email:
        email = email.replace('@', '(at)')
    return email


def format_url(text):
    url = format_string(text)
    parsed = urlparse(url)
    if parsed.scheme in ['http', 'https', 'ftp']:
        pass
    elif parsed.scheme == 'urn':
        url = resolve_urn(url)
    elif parsed.scheme == 'ark':
        url = resolve_ark(url)
    elif parsed.scheme == 'doi' or parsed.path.startswith('10.'):
        url = f"https://doi.org/{parsed.path}"
    elif len(parsed.path) == 19:
        url = resolve_bibcode(url)
    else:
        logging.warning(f"could not parse URL: {url}")
        url = ''
    # check if url is valid
    if url and not is_valid_url(url):
        logging.warning(f"URL is not valid: {url}")
        url = ''
    return url


def resolve_urn(urn):
    if urn.startswith('urn:nbn'):
        url = f'https://nbn-resolving.org/{urn}'
    else:
        url = ''
    return url


def resolve_ark(value):
    if value.startswith('ark:/'):
        # herbadrop uses: https://www.cines.fr
        url = f"https://n2t.net/{value}"
    else:
        url = ''
    return url


def resolve_bibcode(value):
    # TODO: bibcode needs more checking, format: YYYYJJJJJVVVVMPPPPA
    if len(value) == 19:
        url = f'https://ui.adsabs.harvard.edu/abs/{value}'
    else:
        url = ''
    return url
