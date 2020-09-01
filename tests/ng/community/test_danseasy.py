import os

from mdingestion.ng.community.danseasy import DanseasyDatacite

from tests.common import TESTDATA_DIR


def test_rights():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy-oai_datacite', 'SET_1', 'xml', '6ab25834-a565-5d61-b77d-f08f5b3c7b14.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile, url='', community='danseasy', mdprefix='oai_datacite')
    assert ['info:eu-repo/semantics/closedAccess', 'DANS License'] == doc.rights
    assert doc.open_access is False


def test_geo_point():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy-oai_datacite', 'SET_1', 'xml', '989ff5fa-d6d3-52c0-a6c3-41bf01236231.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile, url='', community='danseasy', mdprefix='oai_datacite')
    assert ['info:eu-repo/semantics/openAccess', 'DANS License'] == doc.rights
    assert doc.open_access is True
    assert '(6.2 LON, 52.7 LAT); Plangebied Eekhorstweg 22; Meppel; Drenthe' == doc.spatial_coverage
    assert '{"type":"Point","coordinates": [6.20,52.71]}' == doc.spatial


def test_geo_bbox():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy-oai_datacite', 'SET_1', 'xml', 'dc838a95-fbcc-5fe9-9447-d9d07b80fc21.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile, url='', community='danseasy', mdprefix='oai_datacite')
    assert '(5.5W, 52.0S, 5.5E, 52.0N); Utrecht; Veenendaal: Rondweg west' == doc.spatial_coverage
    assert '{"type":"Polygon","coordinates": [[[5.54,52.01],[5.54,52.01],[5.54,52.01],[5.54,52.01],[5.54,52.01]]]}' == doc.spatial  # noqa


def test_geo_polygon():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy-oai_datacite', 'SET_1', 'xml', '8139b942-59b8-54f2-aa8f-4a07435f7509.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile, url='', community='danseasy', mdprefix='oai_datacite')
    assert '(9.6W, 49.8S, 9.7E, 49.9N); Zutphen; Netherlands' == doc.spatial_coverage
    assert '{"type":"Polygon","coordinates": [[[9.57,49.77],[9.57,49.89],[9.69,49.89],[9.69,49.77],[9.57,49.77]]]}' == doc.spatial  # noqa
