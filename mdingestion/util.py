import datetime
import time
import colander

import logging


def remove_duplicates_from_list(a_list):
    return list(dict.fromkeys(a_list))


def utc2seconds(dt):
    """
    converts datetime to seconds since year 0

    Copyright (C) 2015 Heinrich Widmann.
    Licensed under AGPLv3.
    """
    # Time('0000-12-31 23:00:00', format='iso', scale='utc').unix
    year1epochsec = 62135600400  # 1970-01-01T01:00:00Z
    utc1900 = datetime.datetime.strptime("1900-01-01T11:59:59Z", "%Y-%m-%dT%H:%M:%SZ")
    # utc=self.date2UTC(dt)
    utc = dt
    try:
        utctime = datetime.datetime.strptime(utc, "%Y-%m-%dT%H:%M:%SZ")
        diff = utc1900 - utctime
        diffsec = int(diff.days) * 24 * 60 * 60
        if diff > datetime.timedelta(0):  # date is before 1900
            sec = int(time.mktime((utc1900).timetuple())) - diffsec + year1epochsec
        else:
            sec = int(time.mktime(utctime.timetuple())) + year1epochsec
    except Exception:
        logging.warning(f'utc2seconds date-time {utc} can not converted!')
        return None
    return sec


def is_valid_url(value):
    if colander.url.match_object.match(value) is None:
        return False
    return True


def is_valid_email(value):
    if colander.Email().match_object.match(value) is None:
        return False
    return True
