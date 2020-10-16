import os

from mdingestion.ng.community.pdc import Pdc

from tests.common import TESTDATA_DIR


def test_pdc_dc_1():
    xmlfile = os.path.join(TESTDATA_DIR, 'pdc', 'raw',
                           '64bf054b-5cba-5322-bc32-f2ed88217ac5.xml')
    reader = Pdc()
    doc = reader.read(xmlfile)
    assert 'Devon Island Ice Cap' in doc.title[0]
    assert 'Devon Island Ice Cap core C was recovered from 75.3399°N and 82.6763°W' in doc.description[0]
    assert 'Lisa Hryciw' in doc.creator
    assert 'Polar Data Catalogue' in doc.contributor
    assert 'Chemistry' == doc.discipline
    assert doc.open_access is True
    assert 'metadataPrefix=fgdc&identifier=105_fgdc' in doc.metadata_access
    assert doc.publication_year == '2014'
    assert 'Water chemistry' in doc.keywords
    # assert doc.doi == 'https://doi.org/10.15479/AT:ISTA:92'
    assert doc.source == 'https://www.polardata.ca/pdcsearch/PDCSearchDOI.jsp?doi_id=105'
    assert doc.related_identifier == []
    # <westbc>-84.5</westbc>
    # <eastbc>-79.4</eastbc>
    # <northbc>75.8</northbc>
    # <southbc>74.6</southbc>
    assert doc.spatial_coverage == '(-84.500W, 74.600S, -79.400E, 75.800N)'
    assert doc.spatial == '{"type":"Polygon","coordinates": [[[-84.50,74.60],[-84.50,75.80],[-79.40,75.80],[-79.40,74.60],[-84.50,74.60]]]}'  # noqa
    # <begdate>19490601</begdate>
    # <enddate>20020601</enddate>
    assert doc.temporal_coverage_begin_date == '1949-06-01T00:00:00Z'
    assert doc.temporal_coverage_end_date == '2002-06-01T00:00:00Z'
    # TODO: fails on travis
    # assert doc.temp_coverage_begin == 61485951600
    # assert doc.temp_coverage_end == 63158482800
