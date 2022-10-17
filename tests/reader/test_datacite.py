import os

import pytest

from mdingestion.reader import DataCiteReader

from tests.common import TESTDATA_DIR


def test_common_attributes():
    xml_file = os.path.join(TESTDATA_DIR, 'envidat-datacite', 'raw', 'point_3dea0629-16cb-55b4-8bdb-30d2a57a7fb9.xml')  # noqa
    reader = DataCiteReader()
    doc = reader.read(xml_file, url='https://www.envidat.ch/oai')
    assert 'TRAMM project' in doc.title[0]
    assert 'Rufiberg is a pre-alpine meadow site in Switzerland' in doc.description[0]
    assert 'LANDSLIDES' in doc.keywords
    assert 'https://doi.org/10.16904/5' == doc.doi
    # assert 'https://www.envidat.ch/dataset/10-16904-5' == doc.related_identifier[0]
    assert 'Cornelia Brönnimann (WSL)' == doc.creator[0]
    assert 'ETH Zurich' in doc.publisher[0]
    assert 'Manfred Stähli' == doc.contributor[0]
    # TODO: confusing because publication year returns single value only!
    assert '2015' == doc.publication_year
    assert '2010-11-01T00:00:00Z' == doc.temporal_coverage_begin_date
    assert 'Other (Specified in the description)' in doc.rights
    assert 'Manfred Stähli (Swiss Federal Research Institute WSL)' in doc.contact
    assert 'English' in doc.language
    assert 'Dataset' in doc.resource_type
    assert ['XLSM', 'XLSX'] == doc.format


def test_oai_attributes():
    xml_file = os.path.join(TESTDATA_DIR, 'darus', 'raw', 'Discipline_30fec4d2-1f79-5192-9247-c67154475715.xml')
    reader = DataCiteReader()
    doc = reader.read(xml_file, url='https://darus.uni-stuttgart.de/oai')
    assert 'all' in doc.oai_set
    assert 'doi:10.18419/darus-470' in doc.oai_identifier
    # assert 'https://darus.uni-stuttgart.de/oai?verb=GetRecord&metadataPrefix=oai_datacite&identifier=doi:10.18419/darus-470' == doc.metadata_access  # noqa


def test_doi():
    xml_file = os.path.join(TESTDATA_DIR, 'darus', 'raw', 'Discipline_30fec4d2-1f79-5192-9247-c67154475715.xml')
    reader = DataCiteReader()
    doc = reader.read(xml_file, url='https://darus.uni-stuttgart.de/oai')
    assert 'https://doi.org/10.18419/darus-470' in doc.doi

def test_bbox():
    xml_file = os.path.join(TESTDATA_DIR, 'pangaea', 'raw', '5755f06f-a5a9-5794-9d05-ab23e51452be.xml')
    reader = DataCiteReader()
    doc = reader.read(xml_file, url='https://ws.pangaea.de/oai/provider')
    #   <geoLocationBox>
    # <westBoundLongitude>62.883</westBoundLongitude>
    # <eastBoundLongitude>64.183</eastBoundLongitude>
    # <southBoundLatitude>21.966</southBoundLatitude>
    # <northBoundLatitude>23.15</northBoundLatitude>
    # </geoLocationBox>
    assert '(62.883W, 21.966S, 64.183E, 23.150N); Northern Arabian Sea' == doc.spatial_coverage

def test_point():
    xml_file = os.path.join(TESTDATA_DIR, 'danseasy', 'raw', '989ff5fa-d6d3-52c0-a6c3-41bf01236231.xml')
    reader = DataCiteReader()
    doc = reader.read(xml_file, url='https://easy.dans.knaw.nl/oai')
    assert '(6.197 LON, 52.714 LAT); Plangebied Eekhorstweg 22; Meppel; Drenthe' == doc.spatial_coverage

def test_polygon():
    xml_file = os.path.join(TESTDATA_DIR, 'envidat-datacite', 'raw', '6bd42527-0a4f-563f-88ed-999b1c8ded9e.xml')
    reader = DataCiteReader()
    doc = reader.read(xml_file, url='https://www.envidat.ch/oai')
    assert '(114.000W, -66.000S, 122.000E, -63.000N); Antarctica, Southern Ocean [-66 114 -63 122]' == doc.spatial_coverage
