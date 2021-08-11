import os

from mdingestion.community.inrae import INRAEDatacite

from tests.common import TESTDATA_DIR


def test_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'inrae', 'raw', '79c2c09c-16e6-566f-8be9-e0f700322fce.xml')
    reader = INRAEDatacite()
    doc = reader.read(xmlfile)
    assert 'Health and Life Sciences' in doc.keywords
    assert 'Medicine' in doc.keywords
    assert 'Life Sciences' in doc.discipline
    assert 'General Genetics' in doc.discipline
