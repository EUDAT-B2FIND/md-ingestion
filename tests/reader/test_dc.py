import os

import pytest

from mdingestion.reader import DublinCoreReader

from tests.common import TESTDATA_DIR


def test_dc_slks_point():
    point_file = os.path.join(TESTDATA_DIR, 'slks-oai_dc', 'SET_1', 'xml', 'point_a937f99e-da2a-5c39-ac8d-37e3b0c7e6bd.xml')  # noqa
    reader = DublinCoreReader()
    doc = reader.read(point_file, url='https://www.archaeo.dk/ff/oai-pmh/')
    assert '130511-10 Thors' in doc.title[0]
    assert 'This record describes ancient sites and monuments' in doc.description[0]
    assert 'Rundhøj' in doc.keywords
    assert 'Moesgård Museum' in doc.creator[0]
    assert 'Slots- og Kulturstyrelsen (www.slks.dk)' in doc.publisher[0]
    assert 'Slots- og Kulturstyrelsen (www.slks.dk)' in doc.contact[0]
    assert 'Danish' in doc.language
    assert 'Dataset' in doc.resource_type
    assert 'Other' in doc.discipline
    assert doc.spatial_coverage == 'Thorsø'
