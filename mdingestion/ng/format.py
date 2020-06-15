from dateutil import parser as date_parser
from shapely.geometry import shape
import re


def format(text, type=None):
    if type == 'date':
        formatted_text = format_date(text)
    elif type == 'date_year':
        formatted_text = format_date_year(text)
    else:
        formatted_text = format_string(text)
    return formatted_text


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
    return float(text)


def format_date(text):
    parsed = date_parser.parse(text)
    return parsed.isoformat().split('T')[0]


def format_date_year(text):
    return format_date(text).split('-')[0]
