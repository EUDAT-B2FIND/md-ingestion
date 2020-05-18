import pytest

from .common import Mapper

MAPPER = None


def setup_module(module):
    global MAPPER
    MAPPER = Mapper(community='pangaea', schema='datacite3')
    MAPPER.map()


def test_common_attributes():
    result = MAPPER.load_result(
        filename="point_aa2a1b44-42b2-52ff-98d1-20ab4dae8a18.json",
        set_name="ACD_1")
    # assert result['Contributor'] == ''
    assert result['Publisher'] == ["PANGAEA - Data Publisher for Earth & Environmental Science"]
    assert result['PublicationYear'] == ["2000"]
    assert result['Discipline'] == "Various"
    assert "Discription of coastal shape" in result['title'][0]


def test_spatial_coverage_point():
    result = MAPPER.load_result(
        filename="point_aa2a1b44-42b2-52ff-98d1-20ab4dae8a18.json",
        set_name="ACD_1")
    assert result['SpatialCoverage'] == '(72N,129E)'


def test_spatial_coverage_bbox():
    result = MAPPER.load_result(
        filename="bbox_99eacce5-2984-588e-9ba3-f94c03978983.json",
        set_name="FRAM_1")
    assert result['SpatialCoverage'] == '(79N-80N,4 E-11E)'
