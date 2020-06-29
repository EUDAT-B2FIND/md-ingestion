import os
import pytest

from tests.common import TESTDATA_DIR

from mdingestion.ng import config
from mdingestion.ng.exceptions import CommunityNotFound

HARVEST_LIST = os.path.join(TESTDATA_DIR, 'etc', 'example_harvest_list.csv')


def test_read_harvest_list():
    sources = config.read_harvest_list(HARVEST_LIST)
    assert 'darus' in sources
    assert sources['darus']['community'] == 'darus'
    assert sources['darus']['url'] == 'https://darus.uni-stuttgart.de/oai'
    assert sources['darus']['verb'] == 'ListIdentifiers'
    assert sources['darus']['mdprefix'] == 'oai_datacite'
    assert sources['darus']['mdsubset'] == ''


def test_get_source():
    source = config.get_source(HARVEST_LIST, 'darus')
    assert source['community'] == 'darus'


def test_get_unknown_source():
    with pytest.raises(CommunityNotFound):
        source = config.get_source(HARVEST_LIST, '_unknown_')
