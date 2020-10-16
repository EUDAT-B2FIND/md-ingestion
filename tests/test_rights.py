from mdingestion import rights


def test_is_open_access():
    assert rights.is_open_access('') is True
    assert rights.is_open_access('info:eu-repo/semantics/closedAccess') is False
    assert rights.is_open_access('info:eu-repo/semantics/embargoedAccess') is False
    assert rights.is_open_access('info:eu-repo/semantics/restrictedAccess') is False
    assert rights.is_open_access(
        'Data access is restricted (moratorium, sensitive data, license constraints)',
        closed_access_rights=['restricted']) is False
    assert rights.is_open_access(['info:eu-repo/semantics/closedAccess']) is False
