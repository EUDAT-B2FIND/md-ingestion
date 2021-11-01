import os

from mdingestion.community.bluecloud import Bluecloud

from tests.common import TESTDATA_DIR


def test_json_1():
    jsonfile = os.path.join(TESTDATA_DIR, 'bluecloud', 'raw', 'a65e60b6-7ecc-57b3-a13c-effd515205fb.json')
    reader = Bluecloud()
    doc = reader.read(jsonfile)
    assert '8979 - 5906739 - Argo SIO' in doc.title
