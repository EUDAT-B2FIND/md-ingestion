import os

from mdingestion.ng.community.radar import Radar

from tests.common import TESTDATA_DIR


def test_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'radar-datacite', 'SET_1', 'xml', '4b9e42ae-b56e-5485-9b45-7b00e859a98f.xml')  # noqa
    reader = Radar()
    doc = reader.read(xmlfile)
    assert 'Raw data for' in doc.title[0]
    assert 'Biochemistry' in doc.discipline
    assert 'Deutsche Forschungsgemeinschaft' in doc.funding_reference
    # assert 'CC BY 4.0 Attribution' in doc.rights
    # assert 'https://doi.org/10.22000/81' in doc.doi
