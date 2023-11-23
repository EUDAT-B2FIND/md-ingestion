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

from .linkcheck import ignore_url

import logging

rt_types = [
    'Audiovisual',
    'Book',
    'BookChapter',
    'Dataset',
    'Collection',
    'ComputationalNotebook',
    'ConferencePaper',
    'ConferenceProceeding',
    'DataPaper',
    'Dissertation',
    'Event',
    'Image',
    'InteractiveResource',
    'Journal',
    'JournalArticle',
    'Model',
    'OutputManagementPlan',
    'PeerReview',
    'PhysicalObject',
    'Preprint',
    'Report',
    'Service',
    'Software',
    'Sound',
    'Standard',
    'Text',
    'Workflow',
    'Other']


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


def filter_special_characters(value):
    pattern = re.compile(r'[^a-zA-Z0-9\s]')
    result = pattern.sub('', value)
    return result


def format_value(value, type=None, one=False, min_length=None, max_length=None):
    # work with value list
    values = value or []
    # print("f0", values)
    if not isinstance(values, list):
        values = [values]
    formatted = values
    # print("f1", formatted)
    # format values to type
    formatted = [format(val, type) for val in formatted]
    # print("f2", formatted)
    # drop empty values
    formatted = [val for val in formatted if not is_null_value(val)]
    # print("f3", formatted)
    # formatted = [val for val in formatted if val]
    # remove duplicates
    formatted = remove_duplicates_from_list(formatted)
    # print("f4", formatted)
    if min_length:
        formatted = [val for val in formatted if len(val) >= min_length]
    if max_length:
        formatted = [val[:max_length] for val in formatted]
    # do we have a single value?
    # print("f5", formatted)
    if one:
        if formatted:
            result = formatted[0]
        else:
            result = ''
    else:
        result = formatted
    # print("f6", formatted)
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
    elif type == 'resource_type':
        formatted = format_rt(text)
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
        val = re.findall(r'[0-9\.,-]+', text.strip())[0]
        val = float(val)
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
        val = val.split('+')[0]
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
    elif parsed.scheme == 'hdl':
        url = f"https://hdl.handle.net/{parsed.path}"
    elif parsed.scheme == 'ark':
        url = resolve_ark(url)
    elif parsed.scheme == 'doi' or parsed.path.startswith('10.'):
        url = f"https://doi.org/{parsed.path}"
    elif 'epic' in parsed.path:
        url = f"https://hdl.handle.net/{parsed.path}"
    elif len(parsed.path) == 19:
        url = resolve_bibcode(url)
    elif parsed.path.startswith('10.'):
        url = f"https://doi.org/{parsed.path}"
    else:
        logging.warning(f"could not parse URL: {url}")
        url = ''
    # check if url is valid
    if url and not is_valid_url(url):
        logging.warning(f"URL is not valid: {url}")
        url = ''
    if ignore_url(url):
        logging.warning(f"URL is ignored: {url}")
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


def format_rt(text):
    rt_types_lower = [t.lower() for t in rt_types]
    try:
        index = rt_types_lower.index(text)
        val = rt_types[index]
    except Exception:
        val = text
    return val
