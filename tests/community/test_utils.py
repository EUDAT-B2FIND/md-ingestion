from mdingestion.community import repo, repos
from mdingestion.community import get_repositories

def test_repos():
    assert repos("darus") == ['darus']
    assert repos("pangaea") == ['pangaea']
    assert repos("fmi") == ['fmi']
    # assert communities("b2share") == ['b2share']

def test_get_repositories():
    assert len(get_repositories()) > 100