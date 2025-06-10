import os

from mdingestion.community.uhh import UhhHzsk, UhhUhh

from tests.common import TESTDATA_DIR


def test_rights():
    xmlfile = os.path.join(TESTDATA_DIR, 'uhh', 'raw', '5f7ce430-d4ae-562f-bfb5-505c18721104.xml')  # noqa
    reader = UhhHzsk()
    doc = reader.read(xmlfile)
    assert ['Restricted Access', 'info:eu-repo/semantics/restrictedAccess'] == doc.rights
    assert doc.open_access is False


def test_discipline():
    xmlfile = os.path.join(TESTDATA_DIR, 'uhh', 'raw', 'a8cce4a5-9e38-5e3a-91b9-14cf90f69758.xml')  # noqa
    reader = UhhUhh()
    doc = reader.read(xmlfile)
    assert doc.discipline == ['Other']
