from mdingestion.community import communities, community
from mdingestion.community import get_communities

def test_communities():
    assert communities("darus") == ['darus']
    assert communities("pangaea") == ['pangaea']
    assert communities("fmi") == ['fmi']
    # assert communities("b2share") == ['b2share']

def test_get_communities():
    assert len(get_communities()) > 100