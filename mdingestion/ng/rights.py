CLOSED_ACCESS_RIGHTS = [
    # https://wiki.surfnet.nl/display/standards/info-eu-repo#infoeurepo-AccessRights
    'closedAccess',
    'embargoedAccess',
    'restrictedAccess',
]


def is_open_access(rights, closed_access_rights=None):
    open_access = True
    if rights:
        for lic in CLOSED_ACCESS_RIGHTS:
            if lic in rights:
                open_access = False
                break
        if isinstance(closed_access_rights, list):
            for lic in closed_access_rights:
                if lic in rights:
                    open_access = False
                    break
    return open_access
