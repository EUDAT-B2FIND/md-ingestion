from mdingestion.ng.schema import b2find


def test_b2find_schema():
    cstruct = {
        'title': 'Test',
        'tags': ['testing', 'schema'],
        'description': 'Just at test',
        'source': 'http://localhost/b2f/schema',
    }
    schema = b2find.B2FindSchema()
    schema.deserialize(cstruct)
