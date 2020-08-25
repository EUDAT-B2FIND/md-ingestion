import os

from mdingestion.ng.community.pangaea import PangaeaDatacite

from tests.common import TESTDATA_DIR


def test_pangaea():
    xmlfile = os.path.join(TESTDATA_DIR, 'pangaea-datacite4', 'SET_1', 'xml', 'ef848f90-295c-54e6-b822-c0b66f8bec64.xml')  # noqa
    reader = PangaeaDatacite()
    doc = reader.read(xmlfile, url='https://ws.pangaea.de/oai/provider', community='pangaea', mdprefix='datacite4')
    assert 'https://doi.pangaea.de/10.1594/PANGAEA.904099' == doc.doi
    assert '' == doc.source

def test_pangaea_temporal():
    xmlfile = os.path.join(TESTDATA_DIR, 'pangaea-datacite4', 'SET_1', 'xml', 'e4723633-ab46-5316-875f-fcd788dc0931.xml')  # noqa
    reader = PangaeaDatacite()
    doc = reader.read(xmlfile, url='https://ws.pangaea.de/oai/provider', community='pangaea', mdprefix='datacite4')
    #assert 'https://doi.pangaea.de/10.1594/PANGAEA.904099' == doc.doi
    #assert '' == doc.source
    assert '1999-11-25T00:10:00Z/1999-12-07T16:30:00Z' == doc.temporal_coverage
    assert '1999-11-25T00:10:00Z' == doc.temporal_coverage_begin_date
    assert '1999-12-07T16:30:00Z' == doc.temporal_coverage_end_date
    assert 943488600 == doc.temp_coverage_begin
    assert 944584200 == doc.temp_coverage_end
