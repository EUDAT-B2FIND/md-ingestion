import pytest

from .common import Mapper

MAPPER = None


def setup_module(module):
    global MAPPER
    MAPPER = Mapper(community='envidat', schema='datacite')
    MAPPER.map()


def test_common_attributes():
    result = MAPPER.load_result(
        filename="point_3dea0629-16cb-55b4-8bdb-30d2a57a7fb9.json")
    assert result['Contributor'] == ["EnviDat"]
    assert result['Publisher'] == ["Competence Center Environment and Sustainability, ETH Zurich"]
    assert result['PublicationYear'] == ["2015"]
    assert result['Discipline'] == "Various"
    assert result['title'] == ["TRAMM project - experimental hydrological and hydrogeological dataset of a landslide prone hillslope. Rufiberg, Switzerland"]  # noqa


def test_spatial_coverage_point():
    result = MAPPER.load_result(
        filename="point_3dea0629-16cb-55b4-8bdb-30d2a57a7fb9.json")
    assert result['SpatialCoverage'] == '(47N,9 E)'


def test_spatial_coverage_bbox():
    result = MAPPER.load_result(
        filename="bbox_80e203d7-7c64-5c00-8d1f-a91d49b0fa16.json")
    assert result['SpatialCoverage'] == '(46N-48N,6 E-10E)'


def test_spatial_coverage_point_list():
    result = MAPPER.load_result(
        filename="point_list_9d490baa-18c7-569e-a393-2a5dbd63cec1.json")
    assert result['SpatialCoverage'] == '(47N,8 E)'


@pytest.mark.xfail(reason='does not map polygon')
def test_spatial_coverage_polygon():
    result = MAPPER.load_result(
        filename="polygon_f7160261-c98d-4d49-8966-10c1b0a32831.json")
    assert result['SpatialCoverage'] == ''
