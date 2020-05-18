import pytest

from .common import Mapper

MAPPER = None


def setup_module(module):
    global MAPPER
    MAPPER = Mapper(community='seanoe', schema='oai_dc')
    MAPPER.map()


def test_common_attributes():
    result = MAPPER.load_result(
        filename="bbox_0a476c9c-47ab-51ed-aa7b-8607416cdc0c.json")
    # assert result['Contributor'] == ''
    assert result['Publisher'] == ["SEANOE"]
    assert result['PublicationYear'] == ["2019"]
    assert result['Discipline'] == "Various"
    assert "Updated biological traits" in result['title'][0]


def test_spatial_coverage_bbox():
    result = MAPPER.load_result(
        filename="bbox_0a476c9c-47ab-51ed-aa7b-8607416cdc0c.json")
    assert result['SpatialCoverage'] == '(41N-57N,6 W-11E)'
