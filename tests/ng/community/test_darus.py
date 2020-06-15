import os

from mdingestion.ng.community import DarusDatacite

from tests.common import TESTDATA_DIR


def test_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'darus-oai_datacite', 'SET_1', 'xml', '02baec53-8e79-5611-981e-11df59b824e4.xml')  # noqa
    mapper = DarusDatacite(
        xmlfile, url='https://darus.uni-stuttgart.de/oai', community='darus', mdprefix='oai_datacite')
    result = mapper.json()
    assert 'Deep enzymology data' in result['title'][0]
    assert 'https://doi.org/10.18419/darus-629' == result['DOI']
    assert 'https://darus.uni-stuttgart.de/oai?verb=GetRecord&metadataPrefix=oai_datacite&identifier=doi:10.18419/darus-629' == result['MetaDataAccess']  # noqa
    assert 'Medicine Health and Life Sciences' in [tag['name'] for tag in result['tags']]
    assert 'Medicine' == result['Discipline']
    assert 'application/pdf' in result['Format']
    assert len(result['Format']) == 2
