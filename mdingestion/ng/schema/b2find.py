import colander


class B2FindSchema(colander.MappingSchema):
    title = colander.SchemaNode(
        colander.String(),
        name='title',
        title='Title',
    )
    description = colander.SchemaNode(
        colander.String(),
        name='description',
        title='Description',
        missing=colander.drop,
    )
    tags = colander.SchemaNode(
        colander.Sequence(),
        colander.SchemaNode(colander.String()),
        name='tags',
        title='Tags',
    )
    source = colander.SchemaNode(
        colander.String(),
        name='source',
        title='Source',
        validator=colander.url)
