import os

import pytest

from mdingestion.ng.schema import DublinCore

from tests.common import TESTDATA_DIR


def test_common_attributes():
    point_file = os.path.join(TESTDATA_DIR, 'slks-oai_dc', 'SET_1', 'xml', 'point_a937f99e-da2a-5c39-ac8d-37e3b0c7e6bd.xml')  # noqa
    mapper = DublinCore(point_file)
    result = mapper.json()
    assert '130511-10 Thors' in result['title'][0]
    assert 'This record describes ancient sites and monuments' in result['notes'][0]
    assert 'Rundhøj' in result['tags']
    assert 'http://www.kulturarv.dk/fundogfortidsminder/Lokalitet/34277/' in result['url']
    # assert '???' in result['RelatedIdentifier']
    # assert '??? Museum' in result['MetadataAccess']
    assert 'Moesgård Museum' in result['author'][0]
    # assert '???' in result['Contributor']
    # assert '???' in result['Rights']
    assert 'Slots- og Kulturstyrelsen (www.slks.dk)' in result['Publisher'][0]
    # assert '2018' == result['PublicationYear']
    # assert '???' in result['OpenAccess']
    assert 'Slots- og Kulturstyrelsen (www.slks.dk)' in result['Contact'][0]
    assert 'dan' in result['Language']
    assert 'Dataset' in result['ResourceType']
    assert 'Various' in result['Discipline']
    # assert '???' in result['Format']
    assert 'POINT (9.811246000000001 56.302585)' == result['SpatialCoverage']
    assert "{'type': 'Polygon', 'coordinates': (((9.811246, 56.302585), (9.811246, 56.302585), (9.811246, 56.302585), (9.811246, 56.302585)),)}" == result['spatial']  # noqa
    # assert '2018-12-31' == result['TemporalCoverage']
