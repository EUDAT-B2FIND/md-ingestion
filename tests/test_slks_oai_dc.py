import pytest

from .common import Mapper

MAPPER = None


def setup_module(module):
    global MAPPER
    MAPPER = Mapper(community='slks', schema='oai_dc')
    MAPPER.map()


def test_common_attributes():
    result = MAPPER.load_result(
        filename="point_a937f99e-da2a-5c39-ac8d-37e3b0c7e6bd.json")
    # assert result['Contributor'] == ''
    publisher = result['Publisher']
    publisher.sort()
    assert publisher == ['Aarhus University (www.au.dk)', 'Slots- og Kulturstyrelsen (www.slks.dk)']
    # assert result['PublicationYear'] == ["2019"]
    assert result['Discipline'] == "Archaeology"
    assert "130511-10 Thors" in result['title'][0]


@pytest.mark.xfail(reason='does not map bbox')
def test_spatial_coverage_point():
    result = MAPPER.load_result(
        filename="point_a937f99e-da2a-5c39-ac8d-37e3b0c7e6bd.json")
    assert result['SpatialCoverage'] == ''  # ['9.811246,56.302585']
