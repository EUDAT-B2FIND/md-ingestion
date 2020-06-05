import os

from mdingestion.ng.community import Herbadrop

from tests.common import TESTDATA_DIR


def test_json_1():
    jsonfile = os.path.join(TESTDATA_DIR, 'herbadrop-hjson', 'SET_1', 'hjson', '0d9e8478-3d92-5a5f-92cb-eb678e8e48dd.json')  # noqa
    mapper = Herbadrop(jsonfile)
    result = mapper.json()
    assert 'Gentiana ×marcailhouana Rouy' in result['title'][0]
    assert 'HERBARIUM MUSE' in result['notes'][1]
    assert ['Gentianaceae'] == result['tags']
    assert 'http://coldb.mnhn.fr/catalognumber/mnhn/p/p03945291' == result['url']
    assert 'ark:/87895/1.90-4070723' == result['RelatedIdentifier'][0]
    assert 'P03945291' == result['MetadataAccess'][0]
    assert 'Raynal, J.' == result['author'][0]
    assert 'MNHN' == result['Publisher'][0]
    assert 'CINES' == result['Contributor'][0]
    assert '2019' == result['PublicationYear'][0]
    assert 'http://creativecommons.org/licenses/by/4.0/' == result['Rights'][0]
    assert 'MNHN' == result['Contact'][0]
    assert 'true' == result['OpenAccess']
    assert 'und_UND' == result['Language'][0]
    assert 'StillImage|PRESERVED_SPECIMEN' == result['ResourceType'][0]
    assert 'image/jpeg' == result['Format'][0]
    assert 'Plant Sciences' == result['Discipline']
    assert 'France|Languedoc-Roussillon||||Eyne' == result['SpatialCoverage']
    assert '1968-07-17T00:00:00+01:00Z' == result['TemporalCoverage:BeginDate']
    assert '1968-07-17T00:00:00+01:00Z' == result['TemporalCoverage:EndDate']


def test_json_2():
    jsonfile = os.path.join(TESTDATA_DIR, 'herbadrop-hjson', 'SET_1', 'hjson', '4dec3f1d-0140-5d52-869b-2377d361a786.json')  # noqa
    mapper = Herbadrop(jsonfile)
    result = mapper.json()
    assert 'Senecio Herbarium Practice' in result['title'][0]
    assert 'HERBARIUM MUSE' in result['notes'][1]
