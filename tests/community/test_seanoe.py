import pytest

import os

from mdingestion.community.seanoe import Seanoe

from tests.common import TESTDATA_DIR


def test_seanoe_dc_1():
    xmlfile = os.path.join(TESTDATA_DIR, 'seanoe', 'raw',
                           '4aefaa20-ae37-5c89-bc7a-7574303850c3.xml')
    reader = Seanoe()
    doc = reader.read(xmlfile)
    assert 'Ocean Salinity Stratification (OSS)' in doc.title[0]
    assert 'This dataset is composed by the climatological' in doc.description[0]
    assert "O'Kane, Terence" in doc.creator
    assert 'SEANOE' in doc.publisher
    assert 'Marine Science' in doc.discipline
    assert doc.open_access is True
    assert 'metadataPrefix=oai_dc&identifier=oai:seanoe.org:41101' in doc.metadata_access
    assert doc.publication_year == '2014'
    assert doc.keywords == ['salinity', 'oceanography', 'stratification', 'marine data']
    assert doc.doi == 'https://doi.org/10.17882/41101'
