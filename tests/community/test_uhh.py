import os

from mdingestion.community.uhh import UhhHzsk

from tests.common import TESTDATA_DIR


def test_rights():
    xmlfile = os.path.join(TESTDATA_DIR, 'uhh', 'raw', '5f7ce430-d4ae-562f-bfb5-505c18721104.xml')  # noqa
    reader = UhhHzsk()
    doc = reader.read(xmlfile)
    assert ['Restricted Access', 'info:eu-repo/semantics/restrictedAccess'] == doc.rights
    assert doc.open_access is False
