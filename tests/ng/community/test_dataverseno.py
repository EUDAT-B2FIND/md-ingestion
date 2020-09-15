import os

from mdingestion.ng.community.dataverseno import DataverseNODublinCore

from tests.common import TESTDATA_DIR


def test_dataverseno():
    xmlfile = os.path.join(TESTDATA_DIR, 'dataverseno-oai_dc', 'dataverseno', 'xml', '003d1010-19fb-5232-a762-7de457da5406.xml')  # noqa
    reader = DataverseNODublinCore()
    doc = reader.read(xmlfile, url='', community='dataverseno', mdprefix='oai_dc')
    assert 'https://doi.org/10.18710/DWAC63' == doc.doi
    assert 'Physics;History;History of Science' == doc.discipline
    assert ['DataverseNO'] == doc.publisher
    assert 'EOSC Nordic' in doc.keywords
