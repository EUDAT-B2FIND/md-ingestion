import pytest

import os
import colander

from mdingestion.ng.community import DarusDatacite
from mdingestion.ng.community import Herbadrop
from mdingestion.ng.writer import B2FWriter
from mdingestion.ng.core import B2FSchema

from tests.common import TESTDATA_DIR


def test_b2f_schema():
    cstruct = {
        'title': ['A Title', 'A subtitle'],
        'tags': ['testing', 'schema'],
        'description': ['Just at test'],
        'doi': 'https://doi.org/10.18419/does-not-exist',
        'source': 'http://localhost/b2f/alice_in_wonderland.txt',
        'related_identifier': ['http://localhost/b2f/behind_the_glasses.txt'],
        'creator': ['Alice Blueberry'],
        'publisher': ['Madhatter Strawberry'],
        'contributor': ['White Queen'],
        'publication_year': ['2020'],
        'rights': ['public'],
        'open_access': 'true',
        'contact': ['Alice Blueberry'],
        'language': ['English'],
        'discipline': 'Fiction',
        'spatial_coverage': 'POINT (-121 37)',
        'temporal_coverage': '2020-06-12T12:00:00Z',
    }
    schema = B2FSchema()
    appstruct = schema.deserialize(cstruct)
    assert 'A Title' == appstruct['title'][0]
    assert 'http://localhost/b2f/alice_in_wonderland.txt' == appstruct['source']
    assert 2020 == appstruct['publication_year'][0].year
    assert appstruct['open_access'] is True
    assert 2020 == appstruct['temporal_coverage'].year


def test_b2f_missing_title():
    schema = B2FSchema()
    with pytest.raises(colander.Invalid, match="{'title': 'Required'}"):
        appstruct = schema.deserialize(
            {'description': ['Where is the title?'], 'source': 'http://localhost/some.txt'})


def test_b2f_invalid_date():
    schema = B2FSchema()
    with pytest.raises(colander.Invalid, match="{'publication_year.0': 'Invalid date'}"):
        appstruct = schema.deserialize(
            {'title': ['What year?'], 'source': 'http://localhost/some.txt', 'publication_year': ['yesterday']})


def test_b2f_doc_validation_darus():
    xmlfile = os.path.join(
        TESTDATA_DIR, 'darus-oai_datacite', 'SET_1', 'xml', '02baec53-8e79-5611-981e-11df59b824e4.xml')
    reader = DarusDatacite()
    doc = reader.read(xmlfile, url='https://darus.uni-stuttgart.de/oai', community='darus', mdprefix='oai_datacite')
    writer = B2FWriter()
    cstruct = writer.json(doc)
    schema = B2FSchema()
    appstruct = schema.deserialize(cstruct)
    assert 'Deep enzymology data' in appstruct['title'][0]
    assert 'https://doi.org/10.18419/darus-629' == appstruct['source']
    assert 2020 == appstruct['publication_year'][0].year


def test_b2f_doc_validation_herbadrop():
    jsonfile = os.path.join(TESTDATA_DIR, 'herbadrop-hjson', 'SET_1', 'hjson', '0d9e8478-3d92-5a5f-92cb-eb678e8e48dd.json')  # noqa
    reader = Herbadrop()
    doc = reader.read(jsonfile)
    writer = B2FWriter()
    cstruct = writer.json(doc)
    schema = B2FSchema()
    appstruct = schema.deserialize(cstruct)
    assert 'Gentiana ×marcailhouana Rouy' in appstruct['title'][0]
    assert 'http://coldb.mnhn.fr/catalognumber/mnhn/p/p03945291' == appstruct['source']
    assert 2019 == appstruct['publication_year'][0].year
    assert 'ark:/87895/1.90-4070723' in appstruct['related_identifier'][0]
