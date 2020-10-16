import os

from mdingestion.community.enes import Enes

from tests.common import TESTDATA_DIR


def test_iso19139():
    xmlfile = os.path.join(TESTDATA_DIR, 'enes-iso', 'iso-old-doi', 'xml', 'fff0c660-1eec-5cff-aaaf-498ef7731ad6.xml')  # noqa
    reader = Enes()
    doc = reader.read(xmlfile)

    assert 'http://doi.org/doi:10.1594/WDCC/CMIP5.GIGHr6' == doc.doi
    assert '188183 Mb' in doc.size
    assert 'NASA Goddard Institute for Space Studies' in doc.contact
