import os

from mdingestion.ng.community.ivoa import IVOADatacite

from tests.common import TESTDATA_DIR


def test_vo():
    xmlfile = os.path.join(TESTDATA_DIR, 'ivoa-oai_datacite', 'SET_1', 'xml', 'feeb0259-0155-5718-b5d1-3c37be822004.xml')  # noqa
    reader = IVOADatacite()
    doc = reader.read(xmlfile, url='http://dc.g-vo.org/rr/q/pmh/pubreg.xml', community='ivoa', mdprefix='oai_datacite')
    assert 'Kevin Benson <kmb(at)mssl.ucl.ac.uk>' in doc.contact
    assert 'http://cdaweb.gsfc.nasa.gov/cdaweb/sp_phys/' == doc.source
    assert ['International Virtual Observatory Alliance (IVOA)'] == doc.contributor
    assert '2005AJ....130.2541M' in doc.related_identifier[0]
