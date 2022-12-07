from mdingestion.community import repos, groups

def test_repos():
    assert len(repos()) > 0
    assert repos("darus") == ['darus']
    assert repos("pangaea") == ['pangaea']
    assert repos("fmi") == ['fmi']

def test_groups():
    assert len(groups()) > 0
