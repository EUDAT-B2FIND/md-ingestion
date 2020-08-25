import os

from mdingestion.ng.community.pangaea import PangaeaDatacite

from tests.common import TESTDATA_DIR


def test_pangaea():
    xmlfile = os.path.join(TESTDATA_DIR, 'pangaea-datacite4', 'SET_1', 'xml', 'ef848f90-295c-54e6-b822-c0b66f8bec64.xml')  # noqa
    reader = PangaeaDatacite()
    doc = reader.read(xmlfile, url='https://ws.pangaea.de/oai/provider', community='pangaea', mdprefix='datacite4')
    assert 'https://doi.pangaea.de/10.1594/PANGAEA.904099' == doc.doi
    assert '' == doc.source
