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
