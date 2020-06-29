import os

import pytest

from mdingestion.ng.community import DarusDatacite
from mdingestion.ng.community import Herbadrop
from mdingestion.ng.writer import B2FWriter

from tests.common import TESTDATA_DIR


def test_b2f_darus_oai_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'darus-oai_datacite', 'SET_1', 'xml', '02baec53-8e79-5611-981e-11df59b824e4.xml')  # noqa
    reader = DarusDatacite()
    doc = reader.read(xmlfile, url='https://darus.uni-stuttgart.de/oai', community='darus', mdprefix='oai_datacite')
    writer = B2FWriter()
    result = writer.json(doc)
    assert 'Deep enzymology data' in result['title'][0]
    assert 'https://doi.org/10.18419/darus-629' in result['source']
