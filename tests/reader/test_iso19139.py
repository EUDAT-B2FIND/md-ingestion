import os

import pytest

from mdingestion.reader import ISO19139Reader

from tests.common import TESTDATA_DIR


def test_wdcc_iso19139():
    point_file = os.path.join(
        TESTDATA_DIR, 'wdcc', '009d997f-2742-5636-88f3-a521f444f221.xml')
    reader = ISO19139Reader()
    doc = reader.read(point_file)
    assert 'HD(CP)2 short term observation data of Cloudnet products, HOPE campaign by LACROS' in doc.title[0]
    assert 'Patric Seifert' in doc.creator[0]
    assert 'WDCC' in doc.publisher[0]
    assert '2020' == doc.publication_year
    assert ['Active Remote Sensing'] == doc.keywords
    assert "(6.415W, 50.880S, 6.415E, 50.880N)" == doc.spatial_coverage
    assert '2013-04-01T00:00:00Z' == doc.temporal_coverage_begin_date
    assert '2013-05-31T00:00:00Z' == doc.temporal_coverage_end_date

def test_boundingbox():
    point_file = os.path.join(
        TESTDATA_DIR, 'deims', 'raw', '8708dd68-f413-5414-80fb-da439a4224f9.xml')
    reader = ISO19139Reader()
    doc = reader.read(point_file)
    # <gmd:westBoundLongitude>
    # <gco:Decimal>34.611499754704</gco:Decimal>
    # </gmd:westBoundLongitude>
    # <gmd:eastBoundLongitude>
    # <gco:Decimal>35.343095815055</gco:Decimal>
    # </gmd:eastBoundLongitude>
    # <gmd:southBoundLatitude>
    # <gco:Decimal>29.491402811787</gco:Decimal>
    # </gmd:southBoundLatitude>
    # <gmd:northBoundLatitude>
    # <gco:Decimal>30.968572510749</gco:Decimal>
    # </gmd:northBoundLatitude>
    assert '(34.611W, 29.491S, 35.343E, 30.969N)' == doc.spatial_coverage


@pytest.mark.xfail(reason='missing in reader')
def test_iso19139_temporal_coverage():
    point_file = os.path.join(
        TESTDATA_DIR, 'envidat-iso19139', 'SET_1', 'xml', 'bbox_2ea750c6-4354-5f0a-9b67-2275d922d06f.xml')
    reader = ISO19139Reader()
    doc = reader.read(point_file)
    # assert "POLYGON ((45.81802 10.49203, 45.81802 47.80838, 5.95587 47.80838, 5.95587 10.49203, 45.81802 10.49203))" == doc.spatial_coverage  # noqa
    # assert "{'type': 'Polygon', 'coordinates': (((45.81802, 10.49203), (45.81802, 47.80838), (5.95587, 47.80838), (5.95587, 10.49203), (45.81802, 10.49203)),)}" == doc.spatial  # noqa
    assert '2018-12-31T00:00:00Z' == doc.temporal_coverage_begin_date
    assert '2018-12-31T00:00:00Z' == doc.temporal_coverage_end_date
