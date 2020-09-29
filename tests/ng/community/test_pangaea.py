import os

from mdingestion.ng.community.pangaea import PangaeaDatacite

from tests.common import TESTDATA_DIR


def test_pangaea():
    xmlfile = os.path.join(TESTDATA_DIR, 'pangaea', 'raw', 'ef848f90-295c-54e6-b822-c0b66f8bec64.xml')  # noqa
    reader = PangaeaDatacite()
    doc = reader.read(xmlfile)
    assert 'https://doi.pangaea.de/10.1594/PANGAEA.904099' == doc.doi
    assert '' == doc.source


def test_pangaea_temporal():
    xmlfile = os.path.join(TESTDATA_DIR, 'pangaea', 'raw', 'e4723633-ab46-5316-875f-fcd788dc0931.xml')  # noqa
    reader = PangaeaDatacite()
    doc = reader.read(xmlfile)
    # assert 'https://doi.pangaea.de/10.1594/PANGAEA.904099' == doc.doi
    # assert '' == doc.source
    assert doc.temporal_coverage is None
    assert '1999-11-25T00:10:00Z' == doc.temporal_coverage_begin_date
    assert '1999-12-07T16:30:00Z' == doc.temporal_coverage_end_date
    # TODO: fails on travis
    # assert 63079085400 == doc.temp_coverage_begin
    # assert 63080181000 == doc.temp_coverage_end


def test_pangaea_bbox():
    xmlfile = os.path.join(TESTDATA_DIR, 'pangaea', 'raw', 'c1d81bf1-758f-5b85-a93f-17827fce4900.xml')  # noqa
    reader = PangaeaDatacite()
    doc = reader.read(xmlfile)
    assert 'https://doi.org/10.1594/PANGAEA.557786' == doc.doi
    assert '(0.000W, 78.000S, 7.500E, 80.000N); Fram Strait' == doc.spatial_coverage

def test_pangaea_handle():
    xmlfile = os.path.join(TESTDATA_DIR, 'pangaea', 'raw', '7c0209b8-6189-5bda-89eb-7d5231d6e72d.xml')  # noqa
    reader = PangaeaDatacite()
    doc = reader.read(xmlfile)
    assert 'https://doi.org/10.1594/PANGAEA.472241' == doc.doi
    assert '(-33.649W, -74.241S, -16.500E, -71.869N); Camp Norway; Halley Bay; Weddell Sea; Lyddan Island' == doc.spatial_coverage
    assert 'http://hdl.handle.net/10013/epic.14690.d001' == doc.pid
    assert ['something(at)pangaea.de'] == doc.contact
