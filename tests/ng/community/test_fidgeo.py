import os

from mdingestion.ng.community.fidgeo import FidgeoDatacite

from tests.common import TESTDATA_DIR


def test_fidgeo():
    xmlfile = os.path.join(TESTDATA_DIR, 'fidgeo-oai_datacite', 'DOIDB.FID', 'xml', '90029ec9-dd10-5e6a-b067-7583054a77e6.xml')  # noqa
    reader = FidgeoDatacite()
    doc = reader.read(xmlfile, url='', community='fidgeo', mdprefix='oai_datacite')
    assert ['CC BY 4.0'] == doc.rights
    assert doc.open_access is True
    assert '(67.5W, 25.0S, 95.0E, 45.0N); High Mountain Asia, 1987-2016' == doc.spatial_coverage
