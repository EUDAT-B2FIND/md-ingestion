import os

from mdingestion.community.slks_dc import Slks

from tests.common import TESTDATA_DIR


def test_dublin_core():
    xmlfile = os.path.join(TESTDATA_DIR, 'slks-oai_dc', 'SET_1', 'xml',
                           '5581d3eb-dbca-543f-86e8-8fea5d55db54.xml')
    reader = Slks()
    doc = reader.read(xmlfile)
    assert '080112-47 Mejlø' in doc.title[0]
    assert 'Archaeology' in doc.discipline
    assert doc.open_access is True
    assert '4831' in doc.metadata_access
    assert doc.publication_year == '2020'
    assert doc.spatial_coverage == '(10.615 LON, 55.586 LAT); Mejlø'
    assert doc.temporal_coverage == '250000 BC - 1701 BC; Stenalder; AXXX'
    assert doc.keywords == ['Enkeltfund', 'Single find', 'Settlement', 'Funnsted']
