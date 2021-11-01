import os

from mdingestion.community.bluecloud import Bluecloud

from tests.common import TESTDATA_DIR


def test_json_1():
    jsonfile = os.path.join(TESTDATA_DIR, 'bluecloud', 'raw', 'a65e60b6-7ecc-57b3-a13c-effd515205fb.json')
    reader = Bluecloud()
    doc = reader.read(jsonfile)
    assert ['8979 - 5906739 - Argo SIO'] == doc.title
    assert 'https://data.blue-cloud.org/search-details?step=~012004EC2760F11C211B24C3F35CF95E9FE0E9C4F04' == doc.source
    assert ['RBR_PRES_A', 'RBR_ARGO3', '8979'] == doc.instrument
    assert ['31', 'SIO_IDG', 'ARGO'] == doc.keywords
    assert ['Blue-Cloud', 'EuroArgo – Argo'] == doc.publisher
    assert ['AOML'] == doc.contributor
    assert '2021' == doc.publication_year
    assert '(-164.679W, -2.201S, -164.310E, -2.081N)' == doc.spatial_coverage
    assert '2021-09-26T08:38:34Z' == doc.temporal_coverage_begin_date
    assert '2021-10-09T18:05:02Z' == doc.temporal_coverage_end_date
