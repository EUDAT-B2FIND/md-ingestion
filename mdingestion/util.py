import colander

import logging


def remove_duplicates_from_list(a_list):
    return list(dict.fromkeys(a_list))


def is_valid_url(value):
    if colander.url.match_object.match(value) is None:
        return False
    return True


def is_valid_email(value):
    if colander.Email().match_object.match(value) is None:
        return False
    return True

def convert_to_lon_180(lon):
    # converts longitude value to range [-180, 180]
    #
    # Longitude can be between 0~360 and -180~180.
    #
    # see:
    # https://confluence.ecmwf.int/pages/viewpage.action?pageId=149337515
    if lon > 180 or lon < 0:
        lon_180 = (lon + 180) % 360 - 180
    else:
        lon_180 = lon
    return lon_180 