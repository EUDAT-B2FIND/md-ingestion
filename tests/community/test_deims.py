import os

from mdingestion.community.deims import Deims

from tests.common import TESTDATA_DIR


def test_deims_1():
    xmlfile = os.path.join(TESTDATA_DIR, 'deims', 'raw', '65b17960-85fa-5733-8e28-62af33a8ba8e.xml')
    reader = Deims()
    doc = reader.read(xmlfile)

    assert 'http://hdl.handle.net/11097/217f9245-12e6-4676-af68-2e9d0c27262e' == doc.pid
    assert 'https://deims.org/dataset/a9899fb6-c881-4d3a-a68e-7c6e575c21ac' == doc.source
    assert 'https://deims.org/pycsw/catalogue/csw' in doc.metadata_access


def test_deims_2():
    xmlfile = os.path.join(TESTDATA_DIR, 'deims', 'raw', 'a6249d0b-b617-56ce-b20c-2e102f5ea26c.xml')
    reader = Deims()
    doc = reader.read(xmlfile)

    assert 'http://doi.org/10.34730/8d9f021268d44ac1a78156912d6cb255' == doc.doi
    assert 'https://deims.org/dataset/e562c596-7faa-11e4-a976-005056ab003f' == doc.source
    assert 'https://deims.org/pycsw/catalogue/csw' in doc.metadata_access
    assert 'https://b2share.fz-juelich.de/api/files/bb85b3a0-8b24-466e-8009-4ac85c9f12bc/33723904.xls' in doc.related_identifier  # noqa
