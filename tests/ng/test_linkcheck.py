import os

from mdingestion.ng.reader import DataCiteReader
from mdingestion.ng.linkcheck import LinkChecker

from tests.common import TESTDATA_DIR


def test_linkcheck():
    xml_file = os.path.join(TESTDATA_DIR, 'darus-oai_datacite', 'SET_1', 'xml', 'Discipline_30fec4d2-1f79-5192-9247-c67154475715.xml')  # noqa
    reader = DataCiteReader()
    doc = reader.read(xml_file, url='https://darus.uni-stuttgart.de/oai', mdprefix='oai_datacite')
    lc = LinkChecker()
    lc.add(doc)
