import os

from mdingestion.ng.community import RadarDublinCore

from tests.common import TESTDATA_DIR


def test_dublin_core():
    xmlfile = os.path.join(TESTDATA_DIR, 'radar-oai_dc', 'SET_1', 'xml', '0747e419-cffb-5958-bb39-278df846de13.xml')  # noqa
    reader = RadarDublinCore()
    doc = reader.read(xmlfile)
    assert 'Result files' in doc.title[0]
    assert 'Various' in doc.discipline
    assert 'CC BY 4.0 Attribution' in doc.rights
    assert 'https://doi.org/10.22000/81' in doc.doi
