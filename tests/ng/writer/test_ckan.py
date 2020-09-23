import os

import pytest

from mdingestion.ng.community.darus import DarusDatacite
from mdingestion.ng.community.herbadrop import Herbadrop
from mdingestion.ng.writer import CKANWriter

from tests.common import TESTDATA_DIR


def test_darus_oai_datacite():
    xmlfile = os.path.join(TESTDATA_DIR, 'darus', 'raw', '02baec53-8e79-5611-981e-11df59b824e4.xml')
    reader = DarusDatacite()
    doc = reader.read(xmlfile)
    writer = CKANWriter()
    result = writer.json(doc)
    assert 'Deep enzymology data' in result['title']
    assert '02baec53-8e79-5611-981e-11df59b824e4' == result['name']
    assert 'darus' == result['group']
    assert 'darus' == result['groups'][0]['name']
    assert 'active' == result['state']
    assert '2020-07-01T11:59:59Z' == result['PublicationTimestamp']
    assert 'Deep enzymology data' in result['fulltext']
    assert 'Medicine' in [tag['name'] for tag in result['tags']]
    fields = {}
    for field in result['extras']:
        fields[field['key']] = field['value']
    assert 'true' == fields['OpenAccess']
    # assert '4c034878509472f5514acb44dca9ece16e49b75af515e348610452d941e7a0cd' == result['version']


def test_herbdrop_json():
    jsonfile = os.path.join(TESTDATA_DIR, 'herbadrop', 'raw', '0d9e8478-3d92-5a5f-92cb-eb678e8e48dd.json')
    reader = Herbadrop()
    doc = reader.read(jsonfile)
    writer = CKANWriter()
    result = writer.json(doc)
    assert 'Gentiana ×marcailhouana Rouy' in result['fulltext']
    assert 'StillImage|PRESERVED_SPECIMEN' in result['fulltext']
