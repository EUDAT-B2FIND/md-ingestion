import os

from mdingestion.ng.community.fidgeo import FidgeoDatacite

from tests.common import TESTDATA_DIR


 def test_rights():
    xmlfile = os.path.join(TESTDATA_DIR, 'fidgeo-oai_datacite', 'DOIDB.FID', 'xml', '90029ec9-dd10-5e6a-b067-7583054a77e6.xml') # noqa
    reader = FidgeoDatacite()
    doc = reader.read(xmlfile, url='', community='fidgeo', mdprefix='oai_datacite')
    assert ['info:eu-repo/semantics/closedAccess', 'DANS License'] == doc.rights
    assert doc.open_access is False


def test_geo_point():
    xmlfile = os.path.join(TESTDATA_DIR, 'fidgeo-oai_datacite', 'DOIDB.FID', 'xml', '90029ec9-dd10-5e6a-b067-7583054a77e6.xml')  # noqa
    reader = FidgeoDatacite()
    doc = reader.read(xmlfile, url='', community='fidgeo', mdprefix='oai_datacite')
    assert ['info:eu-repo/semantics/openAccess', 'DANS License'] == doc.rights
    assert doc.open_access is True
    assert '(6.2 LON, 52.7 LAT); Plangebied Eekhorstweg 22; Meppel; Drenthe' == doc.spatial_coverage
    assert '{"type":"Point","coordinates": [6.20,52.71]}' == doc.spatial


def test_geo_bbox():
    xmlfile = os.path.join(TESTDATA_DIR, 'fidgeo-oai_datacite', 'DOIDB.FID', 'xml', '90029ec9-dd10-5e6a-b067-7583054a77e6.xml')  # noqa
    reader = FidgeoDatacite()
    doc = reader.read(xmlfile, url='', community='fidgeo', mdprefix='oai_datacite')
    assert '(5.5W, 52.0S, 5.5E, 52.0N); Utrecht; Veenendaal: Rondweg west' == doc.spatial_coverage
    assert '{"type":"Polygon","coordinates": [[[5.54,52.01],[5.54,52.01],[5.54,52.01],[5.54,52.01],[5.54,52.01]]]}' == doc.spatial  # noqa


def test_geo_polygon():
    xmlfile = os.path.join(TESTDATA_DIR, 'fidgeo-oai_datacite', 'DOIDB.FID', 'xml', '90029ec9-dd10-5e6a-b067-7583054a77e6.xml')  # noqa
    reader = DanseasyDatacite()
    doc = reader.read(xmlfile, url='', community='fidgeo', mdprefix='oai_datacite')
    assert '(9.6W, 49.8S, 9.7E, 49.9N); Zutphen; Netherlands' == doc.spatial_coverage
    assert '{"type":"Polygon","coordinates": [[[9.57,49.77],[9.57,49.89],[9.69,49.89],[9.69,49.77],[9.57,49.77]]]}' == doc.spatial  # noqa
