import os

from mdingestion.ng.community.herbadrop import Herbadrop

from tests.common import TESTDATA_DIR


def test_json_1():
    jsonfile = os.path.join(TESTDATA_DIR, 'herbadrop-hjson', 'SET_1', 'hjson', '0d9e8478-3d92-5a5f-92cb-eb678e8e48dd.json')  # noqa
    reader = Herbadrop()
    doc = reader.read(jsonfile)
    assert 'Gentiana ×marcailhouana Rouy' in doc.title[0]
    # assert 'HERBARIUM MUSE' in doc.description[1]
    assert ['Gentianaceae'] == doc.keywords
    assert 'http://coldb.mnhn.fr/catalognumber/mnhn/p/p03945291' == doc.source
    assert 'https://n2t.net/ark:/87895/1.90-4070723' == doc.related_identifier[0]
    assert 'Raynal, J.' == doc.creator[0]
    assert 'MNHN' == doc.publisher[0]
    assert 'CINES' == doc.contributor[0]
    assert '2019' == doc.publication_year[0]
    assert 'http://creativecommons.org/licenses/by/4.0/' == doc.rights[0]
    assert 'MNHN' == doc.contact[0]
    assert doc.open_access is True
    # assert 'und_UND' == doc.language[0]
    assert 'StillImage|PRESERVED_SPECIMEN' == doc.resource_type[0]
    assert 'image/jpeg' == doc.format[0]
    assert ['3067 bytes', '5108829 bytes'] == doc.size
    assert 'Plant Sciences' == doc.discipline
    # assert 'France|Languedoc-Roussillon||||Eyne' == doc.spatial_coverage
    assert '1968-07-17T00:00:00+01:00Z' == doc.temporal_coverage_begin_date
    assert '1968-07-17T00:00:00+01:00Z' == doc.temporal_coverage_end_date


def test_json_2():
    jsonfile = os.path.join(TESTDATA_DIR, 'herbadrop-hjson', 'SET_1', 'hjson', '4dec3f1d-0140-5d52-869b-2377d361a786.json')  # noqa
    reader = Herbadrop()
    doc = reader.read(jsonfile)
    assert 'Senecio Herbarium Practice' in doc.title[0]
    # assert 'HERBARIUM MUSE' in doc.description[1]


def test_json_metadata_access():
    jsonfile = os.path.join(TESTDATA_DIR, 'herbadrop-hjson', 'SET_1', 'hjson', '0d9e8478-3d92-5a5f-92cb-eb678e8e48dd.json')  # noqa
    reader = Herbadrop()
    doc = reader.read(jsonfile)
    assert 'https://opendata.cines.fr/herbadrop-api/rest/data/mnhnftp/P03945291' in doc.metadata_access


def test_herbadrop_with_pid():
    jsonfile = os.path.join(TESTDATA_DIR, 'herbadrop-hjson', 'SET_1', 'hjson', 'pid_P03068284_6b35f27e-b5d9-526c-b92e-390fa939fa12.json')  # noqa
    reader = Herbadrop()
    doc = reader.read(jsonfile)
    assert 'http://hdl.handle.net/21.T12996/dc6e1c9e-06c6-11ea-99e1-525400276111' == doc.pid
    assert 'https://opendata.cines.fr/herbadrop-api/rest/data/mnhnftp/P03068284' == doc.metadata_access
    assert 'Coronilla juncea L.' in doc.title[0]
    # assert 'HERBARIUM MUSE' in doc.description[1]
    assert ['Fabaceae'] == doc.keywords
    assert 'http://coldb.mnhn.fr/catalognumber/mnhn/p/p03068284' == doc.source
    assert 'https://n2t.net/ark:/87895/1.90-2785993' == doc.related_identifier[0]
    assert 'unavailable' == doc.creator[0]
    assert 'MNHN' == doc.publisher[0]
    assert 'CINES' == doc.contributor[0]
    assert '2019' == doc.publication_year[0]
    assert 'http://creativecommons.org/licenses/by/4.0/' == doc.rights[0]
    assert 'MNHN' == doc.contact[0]
    assert doc.open_access is True
    # assert 'und_UND' == doc.language[0]
    assert 'StillImage|PRESERVED_SPECIMEN' == doc.resource_type[0]
    assert 'image/jpeg' == doc.format[0]
    assert 'Plant Sciences' == doc.discipline
    assert 'HERBARIUM MUSEI PARISIENSIS' == doc.description[2]
    # assert 'France|Languedoc-Roussillon||||Eyne' == doc.spatial_coverage
    # assert '1968-07-17T00:00:00Z' == doc.temporal_coverage_begin_date
    # assert '1968-07-17T00:00:00Z' == doc.temporal_coverage_end_date
