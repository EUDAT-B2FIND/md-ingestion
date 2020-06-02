import os

from mdingestion.ng.community import Herbadrop

from tests.common import TESTDATA_DIR


def test_json():
    jsonfile = os.path.join(TESTDATA_DIR, 'herbadrop-hjson', 'SET_1', 'hjson', '0d9e8478-3d92-5a5f-92cb-eb678e8e48dd.json')  # noqa
    mapper = Herbadrop(jsonfile)
    result = mapper.json()
    assert 'Gentiana ×marcailhouana Rouy' in result['title'][0]
    assert 'unavailable' in result['notes'][0]
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
    assert 'Plant_Sciences' == result['Discipline']
    assert 'France|Languedoc-Roussillon||||Eyne' == result['SpatialCoverage']
    assert '1968-07-17T00:00:00+01:00Z' == result['TemporalCoverage:BeginDate']
    assert '1968-07-17T00:00:00+01:00Z' == result['TemporalCoverage:EndDate']
