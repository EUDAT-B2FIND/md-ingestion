import os

from mdingestion.community.darus import DarusDatacite
from mdingestion.writer import B2FWriter

from tests.common import TESTDATA_DIR


def test_b2f_darus_oai_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'darus', 'raw', '02baec53-8e79-5611-981e-11df59b824e4.xml')
    reader = DarusDatacite()
    doc = reader.read(xmlfile)
    writer = B2FWriter()
    result = writer.json(doc)
    assert 'Deep enzymology data' in result['title'][0]
    assert 'https://doi.org/10.18419/darus-629' in result['doi']
