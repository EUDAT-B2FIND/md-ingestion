CLOSED_ACCESS_RIGHTS = [
    # https://wiki.surfnet.nl/display/standards/info-eu-repo#infoeurepo-AccessRights
    'closedAccess',
    'embargoedAccess',
    'restrictedAccess',
    'restricted',
    'closed',
]


def is_open_access(rights, closed_access_rights=None):
    open_access = True
    if not rights:
        return True
    if isinstance(rights, list):
        _rights = rights
    else:
        _rights = [rights]
    _closed_access_rights = CLOSED_ACCESS_RIGHTS.copy()
    if isinstance(closed_access_rights, list):
        _closed_access_rights.extend(closed_access_rights)
    for lic in _closed_access_rights:
        for _right in _rights:
            if lic.lower() in _right.lower():
                open_access = False
                break
    return open_access
