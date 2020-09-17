import os

import pytest

from mdingestion.ng.reader import DublinCoreReader

from tests.common import TESTDATA_DIR


def test_dc_slks():
    point_file = os.path.join(TESTDATA_DIR, 'slks-oai_dc', 'SET_1', 'xml', 'point_a937f99e-da2a-5c39-ac8d-37e3b0c7e6bd.xml')  # noqa
    reader = DublinCoreReader()
    doc = reader.read(point_file)
    assert '130511-10 Thors' in doc.title[0]
    assert 'This record describes ancient sites and monuments' in doc.description[0]
    assert 'Rundhøj' in doc.keywords
    # assert 'http://www.kulturarv.dk/fundogfortidsminder/Lokalitet/34277/' in doc.source
    # assert '???' in doc.related_identifier
    # assert '??? Museum' in doc.metadata_access
    assert 'Moesgård Museum' in doc.creator[0]
    # assert '???' in doc.contributor
    # assert '???' in doc.rights
    assert 'Slots- og Kulturstyrelsen (www.slks.dk)' in doc.publisher[0]
    # assert '2018' == doc.publication_year
    # assert '???' in doc.open_access
    assert 'Slots- og Kulturstyrelsen (www.slks.dk)' in doc.contact[0]
    assert 'Danish' in doc.language
    assert 'Dataset' in doc.resource_type
    assert 'Various' in doc.discipline
    # assert '???' in doc.format
    # assert 'POINT (9.811246000000001 56.302585)' == doc.spatial_coverage
    # assert "{'type': 'Polygon', 'coordinates': (((9.811246, 56.302585), (9.811246, 56.302585), (9.811246, 56.302585), (9.811246, 56.302585)),)}" == doc.spatial  # noqa
    # assert '2018-12-31' == doc.temporal_coverage


def test_dc_seanoe_bbox():
    xml_file = os.path.join(
        TESTDATA_DIR, 'seanoe-oai_dc', 'SET_1', 'xml', '7d8d61e8-c2c6-5a7e-bef1-24f5a1eb23b5.xml')
    reader = DublinCoreReader()
    doc = reader.read(xml_file)
    # <dc:coverage>North 37.30134, South 37.2888, East -32.275618, West -32.27982</dc:coverage>
    assert doc.spatial_coverage == '(-32.3W, 37.3S, -32.3E, 37.3N)'
