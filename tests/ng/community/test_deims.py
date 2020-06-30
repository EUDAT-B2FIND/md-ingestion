import os

from mdingestion.ng.community import DeimsISO19139

from tests.common import TESTDATA_DIR


def test_iso19139_partone():
    xmlfile = os.path.join(TESTDATA_DIR, 'deims-iso19139', 'full', 'xml', 'fcba5581-4169-509a-ab1e-2634a0928540.xml')  # noqa
    reader = DeimsISO19139()
    doc = reader.read(xmlfile)
    assert 'Aerial counts of waterbirds' in doc.title[0]
    assert 'DEIMS-SDR Site and Dataset registry deims.org' in doc.contributor
    assert 'https://deims.org/dataset/a46d62ee-05fb-11e5-870c-005056ab003f' == doc.source
    assert 'monthly aerial counts of waterbirds in Guadalquivir marshes' in doc.description
    assert 'Long-Term Ecosystem Research in Europe' == doc.publisher[0]
    assert 'https://deims.org/api/datasets/a46d62ee-05fb-11e5-870c-005056ab003f' in doc.related_identifier
    assert 'http://ebd.csic.es/eubon/datasets/Censo+aéreo+1993/be322409-0f52-489f-96bd-b4d990f076db' in doc.related_identifier
