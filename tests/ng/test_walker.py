
import os
import pathlib

from mdingestion.ng import walker

from ..common import TESTDATA_DIR


def test_walk_community():
    my_walker = walker.Walker(TESTDATA_DIR)
    files = [f for f in my_walker.walk_community(community='envidat', mdprefix='datacite', ext='.xml')]
    assert len(files) == 4
    assert 'bbox_80e203d7-7c64-5c00-8d1f-a91d49b0fa16.xml' in sorted(files)[0]


def test_filter_after_date():
    file = pathlib.Path(os.path.join(
        TESTDATA_DIR, 'envidat-datacite', 'SET_1', 'xml', 'bbox_80e203d7-7c64-5c00-8d1f-a91d49b0fa16.xml'))
    assert walker.filter_after_date(file) is True
    assert walker.filter_after_date(file, date=walker.parse_date('2020-05-01')) is True
    assert walker.filter_after_date(file, date=walker.parse_date('2120-05-01')) is False
