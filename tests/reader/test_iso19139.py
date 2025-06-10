import os

import pytest

from mdingestion.reader import ISO19139Reader

from tests.common import TESTDATA_DIR


def test_wdcc_iso19139():
    point_file = os.path.join(
        TESTDATA_DIR, 'wdcc', '009d997f-2742-5636-88f3-a521f444f221.xml')
    reader = ISO19139Reader()
    doc = reader.read(point_file)
    assert 'HD(CP)2 short term observation data of Cloudnet products, HOPE campaign by LACROS' in doc.title[0]
    assert 'Patric Seifert' in doc.creator[0]
    assert 'WDCC' in doc.publisher[0]
    assert '2020' == doc.publication_year
    assert ['Active Remote Sensing'] == doc.keywords
    assert "(6.415W, 50.880S, 6.415E, 50.880N)" == doc.spatial_coverage
    assert '2013-04-01T00:00:00Z' == doc.temporal_coverage_begin_date
    assert '2013-05-31T00:00:00Z' == doc.temporal_coverage_end_date


def test_boundingbox():
    point_file = os.path.join(
        TESTDATA_DIR, 'deims', 'raw', '8708dd68-f413-5414-80fb-da439a4224f9.xml')
    reader = ISO19139Reader()
    doc = reader.read(point_file)
    assert '(34.611W, 29.491S, 35.343E, 30.969N)' == doc.spatial_coverage
