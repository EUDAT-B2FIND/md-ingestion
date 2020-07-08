import os

from mdingestion.ng.community import ESSDatacite

from tests.common import TESTDATA_DIR


def test_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'ess-oai_datacite', 'SET_1', 'xml', '0aa871e7-12c9-5283-8025-562414a73ecd.xml')  # noqa
    reader = ESSDatacite()
    doc = reader.read(xmlfile, url='https://scicat.esss.se/scicat/oai', community='ess', mdprefix='oai_datacite')
    assert 'Sample Data from V20' in doc.title[0]
    assert 'https://github.com/ess-dmsc/ess_file_formats/wiki/HDF5' in doc.description
    assert 'https://doi.org/10.17199/BRIGHTNESS/V200111' == doc.doi
    assert 'https://scicat.esss.se/scicat/oai?verb=GetRecord&metadataPrefix=oai_datacite&identifier=10.17199/BRIGHTNESS/V200111' == doc.metadata_access  # noqa


def test_datacite_no_oai_id():
    xmlfile = os.path.join(TESTDATA_DIR, 'ess-oai_datacite', 'SET_1', 'xml', '6f44650e-3943-54d0-8673-1ceadab50c67_broken_oai_id.xml')  # noqa
    reader = ESSDatacite()
    doc = reader.read(xmlfile, url='https://scicat.esss.se/scicat/oai', community='ess', mdprefix='oai_datacite')
    assert 'Sample Data from NMX' in doc.title[0]
    assert 'https://github.com/ess-dmsc/ess_file_formats/wiki/NMX' in doc.description
    assert 'https://doi.org/10.17199/BRIGHTNESS/NMX0032' == doc.doi
    # assert 'https://scicat.esss.se/scicat/oai?verb=GetRecord&metadataPrefix=oai_datacite&identifier=10.17199/BRIGHTNESS/V200111' == doc.metadata_access  # noqa
    assert 'Pfeiffer, Dorothea (ESS)' in doc.creator
    assert 'ESS' in doc.publisher
    assert 'Photon and neutron data' in doc.tags
    assert 'OpenAccess' in doc.rights
