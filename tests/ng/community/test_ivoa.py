import os

from mdingestion.ng.community.ivoa import IvoaDatacite

from tests.common import TESTDATA_DIR


def test_vo():
    xmlfile = os.path.join(TESTDATA_DIR, 'ivoa-oai_datacite', 'SET_1', 'xml', 'feeb0259-0155-5718-b5d1-3c37be822004.xml')  # noqa
    reader = IvoaDatacite()
    doc = reader.read(xmlfile)
    assert 'Kevin Benson <kmb(at)mssl.ucl.ac.uk>' in doc.contact
    assert 'http://cdaweb.gsfc.nasa.gov/cdaweb/sp_phys/' == doc.source
    assert ['International Virtual Observatory Alliance (IVOA)'] == doc.contributor
    # assert '2005AJ....130.2541M' in doc.related_identifier


def test_vo_2():
    xmlfile = os.path.join(TESTDATA_DIR, 'ivoa-oai_datacite', 'SET_1', 'xml', '19cf53ec-4dc5-53d2-9da3-9b8b2ec2c1b7.xml')  # noqa
    reader = IvoaDatacite()
    doc = reader.read(xmlfile)
    assert 'https://ui.adsabs.harvard.edu/abs/2005AJ....130.2541M' in doc.related_identifier
