import os

from mdingestion.community.danseasy import DanseasyDatacite

from tests.common import TESTDATA_DIR


def test_rights():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy', 'raw', '6ab25834-a565-5d61-b77d-f08f5b3c7b14.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile)
    assert ['info:eu-repo/semantics/closedAccess', 'DANS License', 'https://dans.knaw.nl/en/about/organisation-and-policy/legal-information/DANSLicence.pdf'] == doc.rights
    assert doc.open_access is False


def test_geo_point():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy', 'raw', '989ff5fa-d6d3-52c0-a6c3-41bf01236231.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile)
    assert ['info:eu-repo/semantics/openAccess', 'DANS License', 'https://dans.knaw.nl/en/about/organisation-and-policy/legal-information/DANSLicence.pdf'] == doc.rights
    assert doc.open_access is True
    assert '(6.197 LON, 52.714 LAT); Plangebied Eekhorstweg 22; Meppel; Drenthe' == doc.spatial_coverage
    assert 'POINT (6.1971265600000001 52.7139071100000010)' == doc.wkt


def test_geo_bbox():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy', 'raw', 'dc838a95-fbcc-5fe9-9447-d9d07b80fc21.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile)
    assert '(5.536W, 52.008S, 5.540E, 52.014N); Utrecht; Veenendaal: Rondweg west' == doc.spatial_coverage
    assert "POLYGON ((5.5401004800000004 52.0079704799999973, 5.5401004800000004 52.0141273400000017, 5.5358036500000001 52.0141273400000017, 5.5358036500000001 52.0079704799999973, 5.5401004800000004 52.0079704799999973))" == doc.wkt  # noqa


def test_geo_polygon():
    xmlfile = os.path.join(TESTDATA_DIR, 'danseasy', 'raw', '8139b942-59b8-54f2-aa8f-4a07435f7509.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile)
    assert '(9.569W, 49.770S, 9.693E, 49.886N); Zutphen; Netherlands' == doc.spatial_coverage
    assert 'POLYGON ((9.6823210399999997 49.8048100000000034,' in doc.wkt
