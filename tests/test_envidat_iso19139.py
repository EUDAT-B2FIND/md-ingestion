import pytest

from .common import Mapper

MAPPER = None


def setup_module(module):
    global MAPPER
    MAPPER = Mapper(community='envidat', schema='iso19139')
    MAPPER.map()


def test_common_attributes():
    result = MAPPER.load_result(
        filename="bbox_2ea750c6-4354-5f0a-9b67-2275d922d06f.json")
    assert result['Contributor'] == ["EnviDat"]
    assert result['Publisher'] == ["WSL Institute for Snow and Avalanche Research SLF"]
    assert result['PublicationYear'] == ["2018"]
    assert result['Discipline'] == "Various"
    assert result['title'] == ["Number of avalanche fatalities per hydrological year in Switzerland since 1936-1937"]  # noqa


@pytest.mark.xfail(reason='does not map bbox')
def test_spatial_coverage_bbox():
    result = MAPPER.load_result(
        filename="bbox_2ea750c6-4354-5f0a-9b67-2275d922d06f.json")
    assert result['SpatialCoverage'] == ''  # ['45.81802', '5.95587', '47.80838', '10.49203']


@pytest.mark.xfail(reason='does not map bbox')
def test_spatial_coverage_polygon():
    result = MAPPER.load_result(
        filename="polygon_80cec969-c17d-5023-b99b-a16cdd3ce04d.json")
    assert result['SpatialCoverage'] == ''
