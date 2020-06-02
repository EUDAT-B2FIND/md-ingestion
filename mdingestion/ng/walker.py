import os
import pathlib
import datetime
from dateutil import parser as date_parser

import logging


def parse_date(timestr):
    try:
        parsed_date = date_parser.parse(timestr)
    except Exception:
        parsed_date = None
    return parsed_date


def filter_after_date(path, date=None):
    if not date:
        return True
    mtime = datetime.datetime.fromtimestamp(path.stat().st_mtime)
    logging.debug(f'{mtime} {date}')
    if mtime >= date:
        return True
    return False


class Walker(object):
    def __init__(self, base_dir, ):
        self.base_dir = base_dir

    def walk_community(self, community, mdprefix, ext=None, fromdate=None):
        path = f'{community}-{mdprefix}'
        return self.walk(path, ext, fromdate)

    def walk(self, path, ext=None, fromdate=None):
        ext = ext or '.xml'
        ext_filter = '*' + ext
        date = parse_date(fromdate)

        root_path = pathlib.Path(self.base_dir)
        if path:
            root_path = root_path.joinpath(path)

        for found_path in root_path.rglob(ext_filter):
            if filter_after_date(found_path, date):
                yield found_path.absolute().as_posix()
