from mdingestion.ng import config


def test_to_ignore():
    assert len(config.to_ignore()) > 0
