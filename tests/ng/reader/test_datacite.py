import os

import pytest

from mdingestion.ng.reader import DataCiteReader

from tests.common import TESTDATA_DIR


def test_common_attributes():
    xml_file = os.path.join(TESTDATA_DIR, 'envidat-datacite', 'SET_1', 'xml', 'point_3dea0629-16cb-55b4-8bdb-30d2a57a7fb9.xml')  # noqa
    reader = DataCiteReader()
    doc = reader.read(xml_file)
    assert 'TRAMM project' in doc.title[0]
    assert 'Rufiberg is a pre-alpine meadow site in Switzerland' in doc.description[0]
    assert 'LANDSLIDES' in doc.tags
    assert 'https://doi.org/10.16904/5' == doc.source
    assert 'https://www.envidat.ch/dataset/10-16904-5' == doc.related_identifier[0]
    assert 'Cornelia Brönnimann (WSL)' == doc.creator[0]
    assert 'ETH Zurich' in doc.publisher[0]
    assert 'Manfred Stähli' == doc.contributor[0]
    assert '2015' == doc.publication_year[0]
    assert '2010-11-01T00:00:00Z' == doc.temporal_coverage_begin_date
    assert '2010-11-01T00:00:00Z' == doc.temporal_coverage_end_date
    assert 'Other (Attribution)' in doc.rights
    assert 'Cornelia Brönnimann (WSL)' in doc.contact
    assert 'en' in doc.language
    assert 'Dataset' in doc.resource_type
    assert ['XLSM', 'XLSX'] == doc.format


def test_oai_attributes():
    xml_file = os.path.join(TESTDATA_DIR, 'darus-oai_datacite', 'SET_1', 'xml', 'Discipline_30fec4d2-1f79-5192-9247-c67154475715.xml')  # noqa
    reader = DataCiteReader()
    doc = reader.read(xml_file, url='https://darus.uni-stuttgart.de/oai', mdprefix='oai_datacite')
    assert 'all' in doc.oai_set
    assert 'doi:10.18419/darus-470' in doc.oai_identifier
    assert 'https://darus.uni-stuttgart.de/oai?verb=GetRecord&metadataPrefix=oai_datacite&identifier=doi:10.18419/darus-470' == doc.metadata_access  # noqa


def test_doi():
    xml_file = os.path.join(TESTDATA_DIR, 'darus-oai_datacite', 'SET_1', 'xml', 'Discipline_30fec4d2-1f79-5192-9247-c67154475715.xml')  # noqa
    reader = DataCiteReader()
    doc = reader.read(xml_file)
    assert 'https://doi.org/10.18419/darus-470' in doc.doi


def test_spatial_coverage_point():
    point_file = os.path.join(TESTDATA_DIR, 'envidat-datacite', 'SET_1', 'xml', 'point_3dea0629-16cb-55b4-8bdb-30d2a57a7fb9.xml')  # noqa
    reader = DataCiteReader()
    doc = reader.read(point_file)
    assert 'POINT (47.0889606 8.5544251)' == doc.spatial_coverage
    assert "{'type': 'Polygon', 'coordinates': (((47.0889606, 8.5544251), (47.0889606, 8.5544251), (47.0889606, 8.5544251), (47.0889606, 8.5544251)),)}" == doc.spatial  # noqa


def test_spatial_coverage_bbox():
    point_file = os.path.join(TESTDATA_DIR, 'envidat-datacite', 'SET_1', 'xml', 'bbox_80e203d7-7c64-5c00-8d1f-a91d49b0fa16.xml')  # noqa
    reader = DataCiteReader()
    doc = reader.read(point_file)
    assert 'POLYGON ((47.80838 5.95587, 47.80838 10.49203, 45.81802 10.49203, 45.81802 5.95587, 47.80838 5.95587))' == doc.spatial_coverage  # noqa
    assert "{'type': 'Polygon', 'coordinates': (((47.80838, 5.95587), (47.80838, 10.49203), (45.81802, 10.49203), (45.81802, 5.95587), (47.80838, 5.95587)),)}" == doc.spatial  # noqa
