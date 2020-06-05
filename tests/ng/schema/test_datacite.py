import os

import pytest

from mdingestion.ng.schema import DataCite

from tests.common import TESTDATA_DIR


def test_common_attributes():
    point_file = os.path.join(TESTDATA_DIR, 'envidat-datacite', 'SET_1', 'xml', 'point_3dea0629-16cb-55b4-8bdb-30d2a57a7fb9.xml')  # noqa
    mapper = DataCite(point_file)
    result = mapper.json()
    assert 'TRAMM project' in result['title'][0]
    assert 'Rufiberg is a pre-alpine meadow site in Switzerland' in result['notes'][0]
    assert ['LANDSLIDES', 'SOIL MOISTURE WATER CONTENT', 'WATER TABLE'] == result['tags']
    assert '10.16904/5' == result['url']
    assert 'https://www.envidat.ch/dataset/10-16904-5' == result['RelatedIdentifier'][0]
    assert 'oai:envidat.ch:5333838f-5da6-4574-8d28-ce1794c19631' == result['MetadataAccess'][0]
    assert 'Cornelia Brönnimann' == result['author'][0]
    assert 'ETH Zurich' in result['Publisher'][0]
    assert 'Manfred Stähli' == result['Contributor'][0]
    assert '2015' == result['PublicationYear'][0]
    assert '2010-11-01T00:00:00Z' == result['TemporalCoverage:BeginDate']
    assert '2010-11-01T00:00:00Z' == result['TemporalCoverage:EndDate']


def test_spatial_coverage_point():
    point_file = os.path.join(TESTDATA_DIR, 'envidat-datacite', 'SET_1', 'xml', 'point_3dea0629-16cb-55b4-8bdb-30d2a57a7fb9.xml')  # noqa
    mapper = DataCite(point_file)
    result = mapper.json()
    assert 'POINT (47.0889606 8.5544251)' == result['SpatialCoverage']
    assert "{'type': 'Polygon', 'coordinates': (((47.0889606, 8.5544251), (47.0889606, 8.5544251), (47.0889606, 8.5544251), (47.0889606, 8.5544251)),)}" == result['spatial']  # noqa


def test_spatial_coverage_bbox():
    point_file = os.path.join(TESTDATA_DIR, 'envidat-datacite', 'SET_1', 'xml', 'bbox_80e203d7-7c64-5c00-8d1f-a91d49b0fa16.xml')  # noqa
    mapper = DataCite(point_file)
    result = mapper.json()
    assert 'POLYGON ((47.80838 5.95587, 47.80838 10.49203, 45.81802 10.49203, 45.81802 5.95587, 47.80838 5.95587))' == result['SpatialCoverage']  # noqa
    assert "{'type': 'Polygon', 'coordinates': (((47.80838, 5.95587), (47.80838, 10.49203), (45.81802, 10.49203), (45.81802, 5.95587), (47.80838, 5.95587)),)}" == result['spatial']  # noqa