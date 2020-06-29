import os

from mdingestion.ng.community import SLKSDublinCore

from tests.common import TESTDATA_DIR


def test_dublin_core():
    xmlfile = os.path.join(TESTDATA_DIR, 'slks-oai_dc', 'SET_1', 'xml', 'point_a937f99e-da2a-5c39-ac8d-37e3b0c7e6bd.xml')  # noqa
    reader = SLKSDublinCore()
    doc = reader.read(xmlfile)
    assert '130511-10 Thors' in doc.title[0]
    assert 'Archaeology' in doc.discipline
    assert doc.open_access is True
