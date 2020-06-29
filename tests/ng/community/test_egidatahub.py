import os

from mdingestion.ng.community import EGIDatahubDublinCore

from tests.common import TESTDATA_DIR


def test_dublin_core():
    xmlfile = os.path.join(TESTDATA_DIR, 'egidatahub-oai_dc', 'SET_1', 'xml', '3d0c278c-47d3-5dee-9a56-43b1a5b5d3dd.xml')  # noqa
    reader = EGIDatahubDublinCore()
    doc = reader.read(xmlfile)
    assert 'EGI-DataHub' in doc.title[0]
    assert 'Various' in doc.discipline
    assert 'https://creativecommons.org/licenses/by-nc-nd/4.0/' in doc.rights
    assert 'http://hdl.handle.net/21.T15999/Fw2lYnA' in doc.pid
