import os

from mdingestion.ng.reader import DataCiteReader
from mdingestion.ng.linkcheck import LinkChecker

from tests.common import TESTDATA_DIR


def test_linkcheck():
    xml_file = os.path.join(TESTDATA_DIR, 'darus', 'raw', 'Discipline_30fec4d2-1f79-5192-9247-c67154475715.xml')
    reader = DataCiteReader()
    doc = reader.read(xml_file)
    lc = LinkChecker()
    lc.add(doc)
