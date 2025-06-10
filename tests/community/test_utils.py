from mdingestion.community import repos


def test_repos():
    assert len(repos()) > 0
    assert repos("darus") == ['darus']
    assert repos("pangaea") == ['pangaea']
    assert repos("fmi") == ['fmi']
    assert len(repos("eudat")) > 1
