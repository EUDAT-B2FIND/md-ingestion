import os

import pytest

from mdingestion.ng.reader import ISO19139Reader

from tests.common import TESTDATA_DIR


def test_envidat_iso19139():
    point_file = os.path.join(
        TESTDATA_DIR, 'envidat-iso19139', 'SET_1', 'xml', 'bbox_2ea750c6-4354-5f0a-9b67-2275d922d06f.xml')
    reader = ISO19139Reader()
    doc = reader.read(point_file)
    assert 'Number of avalanche fatalities' in doc.title[0]
    assert 'Avalanche Warning Service SLF' in doc.creator[0]
    assert 'WSL Institute for Snow' in doc.publisher[0]
    assert '2018' == doc.publication_year[0]
    assert ['AVALANCHE ACCIDENT STATISTICS', 'AVALANCHE ACCIDENTS', 'AVALANCHE FATALITIES'] == doc.keywords
    # assert "POLYGON ((45.81802 10.49203, 45.81802 47.80838, 5.95587 47.80838, 5.95587 10.49203, 45.81802 10.49203))" == doc.spatial_coverage  # noqa
    # assert "{'type': 'Polygon', 'coordinates': (((45.81802, 10.49203), (45.81802, 47.80838), (5.95587, 47.80838), (5.95587, 10.49203), (45.81802, 10.49203)),)}" == doc.spatial  # noqa
    # assert '2018-12-31T00:00:00Z' == doc.temporal_coverage_begin_date
    # assert '2018-12-31T00:00:00Z' == doc.temporal_coverage_end_date


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
