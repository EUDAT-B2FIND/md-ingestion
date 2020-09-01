import os

from mdingestion.ng.community.danseasy import DanseasyDatacite

from tests.common import TESTDATA_DIR


def test_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy-oai_datacite', 'SET_1', 'xml', '6ab25834-a565-5d61-b77d-f08f5b3c7b14.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile, url='', community='danseasy', mdprefix='oai_datacite')
    assert ['info:eu-repo/semantics/closedAccess', 'DANS License'] == doc.rights
    assert doc.open_access is False
