import pytest

import colander

from mdingestion.ng.schema import b2find


def test_b2find_schema():
    cstruct = {
        'title': 'Test',
        'tags': ['testing', 'schema'],
        'description': 'Just at test',
        'doi': 'https://doi.org/10.18419/does-not-exist',
        'source': 'http://localhost/b2f/alice_in_wonderland.txt',
        'related_identifier': 'http://localhost/b2f/behind_the_glasses.txt',
        'creator': 'Alice Blueberry',
        'publisher': 'Madhatter Strawberry',
        'contributor': 'White Queen',
        'publication_year': '2020',
        'rights': 'public',
        'open_access': 'true',
        'contact': 'Alice Blueberry',
        'language': 'English',
        'discipline': 'Fiction',
        'spatial_coverage': 'POINT (-121 37)',
        'temporal_coverage': '2020-06-12T12:00:00Z',
    }
    schema = b2find.B2FindSchema()
    appstruct = schema.deserialize(cstruct)
    assert 'Test' == appstruct['title']
    assert 'http://localhost/b2f/alice_in_wonderland.txt' == appstruct['source']
    assert 2020 == appstruct['publication_year'].year
    assert appstruct['open_access'] is True
    assert 2020 == appstruct['temporal_coverage'].year


def test_b2find_missing_title():
    schema = b2find.B2FindSchema()
    with pytest.raises(colander.Invalid, match="{'title': 'Required'}"):
        appstruct = schema.deserialize(
            {'description': 'Where is the title?', 'source': 'http://localhost/some.txt'})


def test_b2find_invalid_date():
    schema = b2find.B2FindSchema()
    with pytest.raises(colander.Invalid, match="{'publication_year': 'Invalid date'}"):
        appstruct = schema.deserialize(
            {'title': 'What year?', 'source': 'http://localhost/some.txt', 'publication_year': 'yesterday'})
