import os

from mdingestion.ng.community.danseasy import DanseasyDatacite

from tests.common import TESTDATA_DIR


def test_rights():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy', 'raw', '6ab25834-a565-5d61-b77d-f08f5b3c7b14.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile)
    assert ['info:eu-repo/semantics/closedAccess', 'DANS License'] == doc.rights
    assert doc.open_access is False


def test_geo_point():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy', 'raw', '989ff5fa-d6d3-52c0-a6c3-41bf01236231.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile)
    assert ['info:eu-repo/semantics/openAccess', 'DANS License'] == doc.rights
    assert doc.open_access is True
    assert '(6.197 LON, 52.714 LAT); Plangebied Eekhorstweg 22; Meppel; Drenthe' == doc.spatial_coverage
    assert '{"type":"Point","coordinates": [6.20,52.71]}' == doc.spatial


def test_geo_bbox():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy', 'raw', 'dc838a95-fbcc-5fe9-9447-d9d07b80fc21.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile)
    assert '(5.536W, 52.008S, 5.540E, 52.014N); Utrecht; Veenendaal: Rondweg west' == doc.spatial_coverage
    assert '{"type":"Polygon","coordinates": [[[5.54,52.01],[5.54,52.01],[5.54,52.01],[5.54,52.01],[5.54,52.01]]]}' == doc.spatial  # noqa


def test_geo_polygon():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy', 'raw', '8139b942-59b8-54f2-aa8f-4a07435f7509.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile)
    assert '(9.569W, 49.770S, 9.693E, 49.886N); Zutphen; Netherlands' == doc.spatial_coverage
    assert '{"type":"Polygon","coordinates": [[[9.57,49.77],[9.57,49.89],[9.69,49.89],[9.69,49.77],[9.57,49.77]]]}' == doc.spatial  # noqa
