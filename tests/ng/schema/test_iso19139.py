import os

import pytest

from mdingestion.ng.schema import ISO19139

from tests.common import TESTDATA_DIR


def test_common_attributes():
    point_file = os.path.join(TESTDATA_DIR, 'envidat-iso19139', 'SET_1', 'xml', 'bbox_2ea750c6-4354-5f0a-9b67-2275d922d06f.xml')  # noqa
    mapper = ISO19139(point_file)
    result = mapper.json()
    assert 'Number of avalanche fatalities' in result['title'][0]
    assert 'Avalanche Warning Service SLF' in result['author'][0]
    assert 'WSL Institute for Snow' in result['Publisher'][0]
    assert '2018' == result['PublicationYear'][0]
    assert "POLYGON ((45.81802 10.49203, 45.81802 47.80838, 5.95587 47.80838, 5.95587 10.49203, 45.81802 10.49203))" == result['SpatialCoverage']  # noqa
    assert "{'type': 'Polygon', 'coordinates': (((45.81802, 10.49203), (45.81802, 47.80838), (5.95587, 47.80838), (5.95587, 10.49203), (45.81802, 10.49203)),)}" == result['spatial']  # noqa
    assert '2018-12-31T00:00:00Z' == result['TemporalCoverage:BeginDate']
    assert '2018-12-31T00:00:00Z' == result['TemporalCoverage:EndDate']
