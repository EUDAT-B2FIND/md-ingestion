import os

from mdingestion.ng.community.dataverseno import DataverseNODublinCore

from tests.common import TESTDATA_DIR


def test_dataverseno():
    xmlfile = os.path.join(TESTDATA_DIR, 'dataverseno-oai_dc', 'dataverseno', 'xml', '003d1010-19fb-5232-a762-7de457da5406.xml')  # noqa
    reader = DataverseNODublinCore()
    doc = reader.read(xmlfile)
    assert 'https://doi.org/10.18710/DWAC63' == doc.doi
    assert 'Physics;History;History of Science' == doc.discipline
    assert ['DataverseNO'] == doc.publisher

def test_dataversenoparttwo():
    xmlfile = os.path.join(TESTDATA_DIR, 'dataverseno-oai_dc', 'dataverseno', 'xml', '6a57038a-9ceb-58f9-a687-947cdca263b5.xml')  # noqa
    reader = DataverseNODublinCore()
    doc = reader.read(xmlfile)
    assert '2020' == doc.publication_year
