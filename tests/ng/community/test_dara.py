import os

from mdingestion.ng.community.dara import DaraRKI, DaraICPSR, DaraSRDA

from tests.common import TESTDATA_DIR


def test_dara_rki():
    # Set: 98, RKI-Bib1 (Robert Koch-Institut)
    xmlfile = os.path.join(TESTDATA_DIR, 'dara_rki', 'raw',
                           '63d15006-0608-5ab2-b96e-688f8b8b8030.xml')
    reader = DaraRKI()
    doc = reader.read(xmlfile)
    assert 'Neuerungen in den aktuellen Empfehlungen' in doc.title[0]
    # assert 'Data on Austrian open access' in doc.description[0]
    assert ['Robert Koch-Institut'] == doc.creator
    assert 'Various' in doc.discipline
    assert doc.open_access is True
    assert 'metadataPrefix=oai_dc&identifier=oai:oai.da-ra.de:461204' in doc.metadata_access
    assert doc.publication_year == '2015'
    assert doc.keywords == []
    assert doc.doi == 'https://doi.org/10.17886/EpiBull-2015-002'
    assert doc.language == ['German']


def test_dara_icpsr():
    # Set: 39, ICPSR - Interuniversity Consortium for Political and Social Research, 39334
    xmlfile = os.path.join(TESTDATA_DIR, 'dara_icpsr', 'raw',
                           'e5c1094e-31c4-51f4-9333-ca4bf029bfd6.xml')
    reader = DaraICPSR()
    doc = reader.read(xmlfile)
    assert 'Integrated Postsecondary Education Data System (IPEDS)' in doc.title[0]
    # assert 'Data on Austrian open access' in doc.description[0]
    assert ['United States Department of Education. National Center for Education Statistics'] == doc.creator
    assert 'Various' in doc.discipline
    assert doc.open_access is True
    assert 'metadataPrefix=oai_dc&identifier=oai:oai.da-ra.de:440957' in doc.metadata_access
    assert doc.publication_year == '2004'
    assert 'academic degrees' in doc.keywords
    assert doc.doi == 'https://doi.org/10.3886/ICPSR06936'
    assert doc.language == ['English']


def test_dara_srda():
    # Set: 128, SRDA - Survey Research Data Archive Taiwan, 2680
    xmlfile = os.path.join(TESTDATA_DIR, 'dara_srda', 'raw',
                           'dd0e0d72-ce76-510b-bbf0-217c20784e2f.xml')
    reader = DaraSRDA()
    doc = reader.read(xmlfile)
    assert 'Taiwan Fertility and Family Survey, 1998(Restricted Access Data)' in doc.title[0]
    # assert 'Data on Austrian open access' in doc.description[0]
    assert ['Health Promotion Administration, Ministry of Health and Welfare'] == doc.creator
    assert 'Various' in doc.discipline
    assert doc.open_access is True
    assert 'metadataPrefix=oai_dc&identifier=oai:oai.da-ra.de:549364' in doc.metadata_access
    assert doc.publication_year == '2014'
    assert doc.keywords == []
    assert doc.doi == 'https://doi.org/10.6141/TW-SRDA-R040001-1'
    assert doc.language == []
