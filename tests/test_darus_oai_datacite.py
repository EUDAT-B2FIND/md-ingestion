import pytest

from .common import Mapper

MAPPER = None


def setup_module(module):
    global MAPPER
    MAPPER = Mapper(community='darus', schema='oai_datacite')
    MAPPER.map()


def test_common_attributes():
    result = MAPPER.load_result(
        filename="Discipline_30fec4d2-1f79-5192-9247-c67154475715.json")
    # assert result['Contributor']== ['Braun, Thorsten', 'Universität Stuttgart']
    assert result['Publisher'] == ["DaRUS"]
    assert result['PublicationYear'] == ["2019"]
    assert "Evaluation" in result['title'][0]


@pytest.mark.xfail(reason='does not map discipline Social Sciences')
def test_discipline():
    result = MAPPER.load_result(
        filename="Discipline_30fec4d2-1f79-5192-9247-c67154475715.json")
    assert result['Discipline'] == 'Social Sciences'
