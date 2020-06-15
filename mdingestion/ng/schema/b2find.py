import colander


class B2FindSchema(colander.MappingSchema):
    title = colander.SchemaNode(
        colander.String(),
        name='title',
        title='Title',
        description='A name or title by which a resource is known.'
    )
    description = colander.SchemaNode(
        colander.String(),
        name='description',
        title='Description',
        description='An additional information describing the content of the resource. Could be an abstract, a summary or a Table of Content.',  # noqa
        missing=colander.drop,
    )
    tags = colander.SchemaNode(
        colander.Sequence(),
        colander.SchemaNode(colander.String()),
        name='tags',
        title='Tags',
        description='A subject, keyword, classification code, or key phrase describing the content.',
        missing=colander.drop,
    )
    doi = colander.SchemaNode(
        colander.String(),
        name='doi',
        title='DOI',
        description='A persistent, citable identifier that uniquely identifies a resource.',
        missing=colander.drop,
        validator=colander.url,
    )
    source = colander.SchemaNode(
        colander.String(),
        name='source',
        title='Source',
        description='An identifier (URL) that uniquely identifies a resource. It may link to the data itself or a landing page that curates the data.',  # noqa
        validator=colander.url,
    )
    related_identifier = colander.SchemaNode(
        colander.String(),
        name='related_identifier',
        title='RelatedIdentifier',
        description='A link to related resources or supplements.',
        missing=colander.drop,
        validator=colander.url,
    )
    metadata_access = colander.SchemaNode(
        colander.String(),
        name='metadata_access',
        title='MetaDataAccess',
        description='Is a link to the originally harvested metadata record (GetRecord request).',
        missing=colander.drop,
        validator=colander.url,
    )
    creator = colander.SchemaNode(
        colander.String(),
        name='creator',
        title='Creator',
        description='The main researchers involved in producing the data, or the authors of the publication, or the measurement or monitoring station that produces the data in priority order.',  # noqa
        missing=colander.drop,
    )
    publisher = colander.SchemaNode(
        colander.String(),
        name='publisher',
        title='Publisher',
        description='The name of the entity that holds, archives, publishes prints, distributes, releases, issues, or produces the resource. This property will be used to formulate the citation, so consider the prominence of the role.',  # noqa
        missing=colander.drop,
    )
    contributor = colander.SchemaNode(
        colander.String(),
        name='contributor',
        title='Contributor',
        description='The institution or person responsible for collecting, managing, distributing, or otherwise contributing to the development of the resource.',  # noqa
        missing=colander.drop,
    )
    publication_year = colander.SchemaNode(
        colander.Date(),
        name='publication_year',
        title='PublicationYear',
        description='The institution or person responsible for collecting, managing, distributingThe year when the data was or will be made publicly available.',  # noqa
        missing=colander.drop,
    )
    rights = colander.SchemaNode(
        colander.String(),
        name='rights',
        title='Rights',
        description='Any rights information for this resource.',  # noqa
        missing=colander.drop,
    )
    open_access = colander.SchemaNode(
        colander.Boolean(),
        name='open_access',
        title='OpenAccess',
        description='Is the dataset openly accessible or not.',  # noqa
        missing=colander.drop,
    )
    contact = colander.SchemaNode(
        colander.String(),
        name='contact',
        title='Contact',
        description='Any contact information for this resource.',  # noqa
        missing=colander.drop,
    )
    language = colander.SchemaNode(
        colander.String(),
        name='language',
        title='Language',
        description='The primary language of the resource.',  # noqa
        missing=colander.drop,
    )
    resource_type = colander.SchemaNode(
        colander.String(),
        name='resource_type',
        title='ResourceType',
        description='A description of the resource, a general type of the resource.',  # noqa
        missing=colander.drop,
    )
    format = colander.SchemaNode(
        colander.String(),
        name='format',
        title='Format',
        description='The mimetype of the resource.',  # noqa
        missing=colander.drop,
    )
    discipline = colander.SchemaNode(
        colander.String(),
        name='discipline',
        title='Discipline',
        description='A scientific discipline the resource originates from. A closed vocabulary is used.',  # noqa
        missing=colander.drop,
    )
    spatial_coverage = colander.SchemaNode(
        colander.String(),
        name='spatial_coverage',
        title='SpatialCoverage',
        description='A geolocation the research data itself is related to. Content of this category is displayed in plain text.  If a longitude/latitude information is given it will be displayed at the map.',  # noqa
        missing=colander.drop,
    )
    temporal_coverage = colander.SchemaNode(
        colander.DateTime(),
        name='temporal_coverage',
        title='TemporalCoverage',
        description='Period of time the research data itself is related to. Could be a date format or plain text.',  # noqa
        missing=colander.drop,
    )
