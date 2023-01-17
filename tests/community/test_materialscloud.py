import pytest

import os

from mdingestion.community.materialscloud import MaterialscloudDublinCore

from tests.common import TESTDATA_DIR


@pytest.mark.xfail(reason="doi fails")
def test_dublin_core():
    xmlfile = os.path.join(
        TESTDATA_DIR,
        'materialscloud-oai_dc', 'full', 'xml',
        '013459fc-ad63-5bd7-a580-c882c925dcfa.xml')
    reader = MaterialscloudDublinCore()
    doc = reader.read(xmlfile)
    assert 'Balancing DFT' in doc.title[0]
    assert 'Materials Science and Engineering' in doc.discipline
    assert doc.open_access is True
    assert 'https://doi.org/10.24435/materialscloud:2020.0012/v1' == doc.doi
    assert 'https://archive.materialscloud.org/record/2020.0012/v1' == doc.source
