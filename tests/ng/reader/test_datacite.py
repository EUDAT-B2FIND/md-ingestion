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
    assert 'LANDSLIDES' in doc.keywords
    assert 'https://doi.org/10.16904/5' == doc.doi
    # assert 'https://www.envidat.ch/dataset/10-16904-5' == doc.related_identifier[0]
    assert 'Cornelia Brönnimann (WSL)' == doc.creator[0]
    assert 'ETH Zurich' in doc.publisher[0]
    assert 'Manfred Stähli' == doc.contributor[0]
    assert '2015' == doc.publication_year[0]
    assert '2010-11-01T00:00:00Z' == doc.temporal_coverage_begin_date
    assert 'Other (Specified in the description)' in doc.rights
    assert 'Manfred Stähli (Swiss Federal Research Institute WSL)' in doc.contact
    assert 'English' in doc.language
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
