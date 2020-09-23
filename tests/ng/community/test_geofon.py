import os

from mdingestion.ng.community.geofon import GeofonDatacite

from tests.common import TESTDATA_DIR


def test_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'geofon-oai_datacite', 'DOIDB.GEOFON', 'xml', '56347bce-8b50-51e9-9564-f99269b46c48.xml')  # noqa
    reader = GeofonDatacite()
    doc = reader.read(xmlfile)
    assert 'GEOFON event gfz2012mfau' in doc.title[0]
    assert 'https://doi.org/10.5880/GEOFON.gfz2012mfau' == doc.doi
    assert 'http://doidb.wdc-terra.org/oaip/oai?verb=GetRecord&metadataPrefix=oai_datacite&identifier=oai:doidb.wdc-terra.org:4317' == doc.metadata_access  # noqa
    assert 'Seismology' == doc.discipline
    assert 'Deutsches GeoForschungsZentrum GFZ' in doc.contributor
    # assert 'application/pdf' in doc.format
    # assert len(doc.format) == 2
    assert doc.open_access is True
    assert '2012-06-22T00:00:00Z' in doc.temporal_coverage_begin_date
