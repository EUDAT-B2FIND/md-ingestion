import os

from mdingestion.reader import DataCiteReader
from mdingestion.linkcheck import LinkChecker, ignore_url

from tests.common import TESTDATA_DIR


def test_linkcheck():
    xml_file = os.path.join(TESTDATA_DIR, 'darus', 'raw', 'Discipline_30fec4d2-1f79-5192-9247-c67154475715.xml')
    reader = DataCiteReader()
    doc = reader.read(xml_file)
    lc = LinkChecker()
    lc.add(doc)


def test_ignore_url():
    assert ignore_url('https://www.leo.org') is False
    assert ignore_url(
        'https://www.umweltbundesamt.at/en/services/services_pollutants/services_airquality/en_ref_zoebelboden/')
    assert ignore_url(
        'http://ebd.csic.es/eubon/datasets/Censo+aéreo+1993/be322409-0f52-489f-96bd-b4d990f076db'
    )
