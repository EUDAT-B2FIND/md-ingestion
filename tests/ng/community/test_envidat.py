import os

from mdingestion.ng.community import EnvidatDatacite, EnvidatISO19139

from tests.common import TESTDATA_DIR


def test_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'envidat-datacite', 'SET_1', 'xml', 'point_3dea0629-16cb-55b4-8bdb-30d2a57a7fb9.xml')  # noqa
    mapper = EnvidatDatacite(xmlfile)
    result = mapper.json()
    assert 'TRAMM project' in result['title'][0]
    assert 'EnviDat' in result['Contributor']


def test_iso19139():
    xmlfile = os.path.join(TESTDATA_DIR, 'envidat-iso19139', 'SET_1', 'xml', 'bbox_2ea750c6-4354-5f0a-9b67-2275d922d06f.xml')  # noqa
    mapper = EnvidatISO19139(xmlfile)
    result = mapper.json()
    assert 'Number of avalanche fatalities' in result['title'][0]
    assert 'EnviDat' in result['Contributor']
