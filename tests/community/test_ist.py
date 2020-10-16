import os

from mdingestion.community.ist import IstDublinCore

from tests.common import TESTDATA_DIR


def test_ist_dc_1():
    xmlfile = os.path.join(TESTDATA_DIR, 'ist', 'raw',
                           '4710d803-19e2-5381-80ee-740a868f8428.xml')
    reader = IstDublinCore()
    doc = reader.read(xmlfile)
    assert 'SAGE Austrian Publications 2013-2017' in doc.title[0]
    assert 'Data on Austrian open access' in doc.description[0]
    assert doc.creator == ['Villányi, Márton']
    assert 'Life Sciences, Natural Sciences, Engineering Sciences' in doc.discipline
    assert doc.open_access is True
    assert 'oai:pub.research-explorer.app.ist.ac.at:5580' in doc.metadata_access
    assert doc.publication_year == '2018'
    assert doc.keywords == ['Publication analysis', 'Bibliography', 'Open Access', 'ddc 020']
    assert doc.doi == 'https://doi.org/10.15479/AT:ISTA:92'
    assert doc.source == 'https://research-explorer.app.ist.ac.at/record/5580'
