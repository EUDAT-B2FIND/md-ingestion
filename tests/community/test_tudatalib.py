import os

from mdingestion.community.tudatalib import TudatalibDatacite

from tests.common import TESTDATA_DIR


def test_keywords():
    xmlfile = os.path.join(
        TESTDATA_DIR, 'tudatalib', 'raw', '218bf893-c8cf-5a8c-aad1-09a351e37da8.xml')
    reader = TudatalibDatacite()
    doc = reader.read(xmlfile)
    assert 'Functional printing' in doc.keywords  # subject with subjectScheme
    assert 'Weiche Materie' in doc.keywords  # include only DFG
    assert '620' not in doc.keywords # exclude DDC
