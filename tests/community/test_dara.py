import os

from mdingestion.community.dara import DaraRKI, DaraSRDA, DaraGESIS

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
    assert 'Public Health' in doc.discipline[0]
    assert doc.open_access is True
    assert 'metadataPrefix=oai_dc&identifier=oai:oai.da-ra.de:461204' in doc.metadata_access
    assert doc.publication_year == '2015'
    assert doc.keywords == []
    assert doc.doi == 'https://doi.org/10.17886/EpiBull-2015-002'
    assert doc.language == ['German']


def test_dara_srda():
    # Set: 128, SRDA - Survey Research Data Archive Taiwan, 2680
    xmlfile = os.path.join(TESTDATA_DIR, 'dara_srda', 'raw',
                           'dd0e0d72-ce76-510b-bbf0-217c20784e2f.xml')
    reader = DaraSRDA()
    doc = reader.read(xmlfile)
    assert 'Taiwan Fertility and Family Survey, 1998(Restricted Access Data)' in doc.title[0]
    # assert 'Data on Austrian open access' in doc.description[0]
    assert ['Health Promotion Administration, Ministry of Health and Welfare'] == doc.creator
    assert 'Empirical Social Research' in doc.discipline
    assert doc.open_access is True
    assert 'metadataPrefix=oai_dc&identifier=oai:oai.da-ra.de:549364' in doc.metadata_access
    assert doc.publication_year == '2014'
    assert doc.keywords == []
    assert doc.doi == 'https://doi.org/10.6141/TW-SRDA-R040001-1'
    assert doc.language == []


def test_dara_gesis():
    xmlfile = os.path.join(TESTDATA_DIR, 'dara_gesis', 'raw',
                           'd9bf3e91-9a61-51cd-94bd-c082a7b9c17f.xml')
    reader = DaraGESIS()
    doc = reader.read(xmlfile)
    assert ['Rothenbacher, Franz'] == doc.creator
    assert 'Social Sciences' in doc.discipline
    assert doc.rights[0] == 'All metadata from GESIS DBK are available free of restriction under the Creative Commons CC0 1.0 Universal Public Domain Dedication. However, GESIS requests that you actively acknowledge and give attribution to all metadata sources, such as the data providers and any data aggregators, including GESIS. For further information see https://dbk.gesis.org/dbksearch/guidelines.asp'
    assert 'German Empire from 1971 to 1945. Federal German Repuclib from 1949 to 1975. Prussia from 1817 to 1900' in doc.places
