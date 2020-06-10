import os

from mdingestion.ng.community import ESSDatacite

from tests.common import TESTDATA_DIR


def test_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'ess-oai_datacite', 'SET_1', 'xml', '0aa871e7-12c9-5283-8025-562414a73ecd.xml')  # noqa
    mapper = ESSDatacite(xmlfile, 'https://scicat.esss.se/scicat/oai', 'oai_datacite')
    result = mapper.json()
    assert 'Sample Data from V20' in result['title'][0]
    assert 'https://github.com/ess-dmsc/ess_file_formats/wiki/HDF5' in result['notes']
    assert 'https://doi.org/10.17199/BRIGHTNESS/V200111' == result['DOI']
    assert 'https://scicat.esss.se/scicat/oai?verb=GetRecord&metadataPrefix=oai_datacite&identifier=10.17199/BRIGHTNESS/V200111' == result['MetadataAccess']  # noqa
