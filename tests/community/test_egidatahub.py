import os

from mdingestion.ng.community.egidatahub import EgidatahubDublinCore

from tests.common import TESTDATA_DIR


def test_dublin_core():
    xmlfile = os.path.join(TESTDATA_DIR, 'egidatahub-oai_dc', 'SET_1', 'xml', '3d0c278c-47d3-5dee-9a56-43b1a5b5d3dd.xml')  # noqa
    reader = EgidatahubDublinCore()
    doc = reader.read(xmlfile)
    assert 'EGI-DataHub' in doc.title[0]
    assert 'Various' in doc.discipline
    assert 'https://creativecommons.org/licenses/by-nc-nd/4.0/' in doc.rights
    assert 'http://hdl.handle.net/21.T15999/Fw2lYnA' == doc.pid
    assert 'https://datahub.egi.eu/share/dec6359cdf03b3a7405ac75b70a4cecb' == doc.source
    assert doc.open_access is True
    assert 'EGI' in doc.keywords
