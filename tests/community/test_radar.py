import os

from mdingestion.community.radar import Radar

from tests.common import TESTDATA_DIR


def test_radar_1():
    xmlfile = os.path.join(TESTDATA_DIR, 'radar', 'raw', '4b9e42ae-b56e-5485-9b45-7b00e859a98f.xml')
    reader = Radar()
    doc = reader.read(xmlfile)
    assert 'Raw data for' in doc.title[0]
    assert 'Biochemistry' in doc.discipline
    assert 'Deutsche Forschungsgemeinschaft' in doc.funding_reference
    # assert 'CC BY 4.0 Attribution' in doc.rights
    # assert 'https://doi.org/10.22000/81' in doc.doi


def test_radar_2():
    xmlfile = os.path.join(TESTDATA_DIR, 'radar', 'raw', '2324e30b-85bc-5982-b879-570d61fe065c.xml')
    reader = Radar()
    doc = reader.read(xmlfile)
    assert 'Supplementary data for' in doc.title[0]
    assert 'Chemistry;Life Sciences' == doc.discipline
    assert ['Comisión Nacional de Investigación Científica y Tecnológica',
            'Bộ Giáo dục và Ðào tạo'] == doc.funding_reference

def test_radar_3():
    xmlfile = os.path.join(TESTDATA_DIR, 'radar', 'raw', 'd812138e-2e73-5dd3-8258-fc783f47db39.xml')
    reader = Radar()
    doc = reader.read(xmlfile)
    assert '2020' == doc.publication_year
