import colander


class B2FSchema(colander.MappingSchema):
    community = colander.SchemaNode(
        colander.String(),
        name='community',
        title='Community',
        description='The scientific community, Research Infrastructure, Project or Data provider from which B2FIND harvests the metadata.',  # noqa
        validator=colander.Length(min=1),
    )
    identifier = colander.SchemaNode(
        colander.String(),
        name='identifier',
        title='Identifier',
        description='A unique string that identifies a resource, ideally persistent. It may be a DOI, a PID or any other source.',  # noqa
        validator=colander.url,
    )
    title = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), validator=colander.Length(min=3)),
        name='title',
        title='Title',
        description='A name or title by which a resource is known.',
        validator=colander.Length(min=1)
    )
    description = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='description',
        title='Description',
        description='All additional information that does not fit in any of the other categories. May be used for technical information. It is a best practice to supply a description.',  # noqa
        missing=colander.drop,
    )
    # TODO: decide how it is named in EUDAT core
    keyword = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='keyword',
        title='Keyword',
        description='Subject, keyword, classification code, or key phrase describing the resource. Could be free text.',
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
    pid = colander.SchemaNode(
        colander.String(),
        name='pid',
        title='PID',
        description='A persistent identifier that uniquely identifies a resource.',
        missing=colander.drop,
        validator=colander.url,
    )
    source = colander.SchemaNode(
        colander.String(),
        name='source',
        title='Source',
        description='An identifier that uniquely identifies a resource. It may link to the data itself or a landing page that curates the data.',  # noqa
        missing=colander.drop,
        validator=colander.url,
    )
    # TODO: related_identifier url validation is not working ... herbadrop, darus, ...
    related_identifier = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='related_identifier',
        title='RelatedIdentifier',
        description='Identifiers of related resources.',
        missing=colander.drop,
        # validator=colander.url,
    )
    metadata_access = colander.SchemaNode(
        colander.String(),
        name='metadata_access',
        title='MetaDataAccess',
        description='Is a link to the originally harvested metadata record.',
        missing=colander.drop,
        validator=colander.url,
    )
    creator = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='creator',
        title='Creator',
        description='The main researchers involved working on the data, or the authors of the publication in priority order. May be a corporate/institutional or personal name.',  # noqa
        missing=colander.drop,
    )
    publisher = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='publisher',
        title='Publisher',
        description='The name of the entity that holds, archives, publishes prints, distributes, releases, issues, or produces the resource. This property will be used to formulate the citation, so consider the prominence of the role.',  # noqa
        missing=colander.drop,
    )
    contributor = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='contributor',
        title='Contributor',
        description='The institution or person responsible for collecting, managing, distributing, or otherwise contributing to the development of the resource.',  # noqa
        missing=colander.drop,
    )
    instrument = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='instrument',
        title='Instrument',
        description='The technical instrument(s) used to generate (observe or measure) the data.',  # noqa
        missing=colander.drop,
    )
    publication_year = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.Date(), missing=colander.drop),
        name='publication_year',
        title='PublicationYear',
        description='Year when the data is made publicly available. If an embargo period has been in effect, use the date when the embargo period ends.',  # noqa
        missing=colander.drop,
    )
    funding_reference = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='funding_reference',
        title='FundingReference',
        description='Information about financial support (funding) for the resource.',
        missing=colander.drop,
    )
    rights = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='rights',
        title='Rights',
        description='Any rights information for this resource.',
        missing=colander.drop,
    )
    open_access = colander.SchemaNode(
        colander.Boolean(),
        name='open_access',
        title='OpenAccess',
        description='Information on whether the resource is openly accessible or not.',
        missing=colander.drop,
    )
    contact = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='contact',
        title='Contact',
        description='A reference to contact information.',
        missing=colander.drop,
    )
    # TODO: we would like to use language mapping to ISO codes.
    language = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='language',
        title='Language',
        description='Language(s) of the resource.',
        missing=colander.drop,
    )
    resource_type = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='resource_type',
        title='ResourceType',
        description='The type(s) of the resource. Free text values allowed.',
        missing=colander.drop,
    )
    format = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='format',
        title='Format',
        description='Technical format of the resource. Free text. Use file extension or MIME type where possible, e.g. PDF, XML, MPG or application/pdf, text/xml, video/mpeg.',
        missing=colander.drop,
    )
    size = colander.SchemaNode(
        colander.Sequence(accept_scalar=True),
        colander.SchemaNode(colander.String(), missing=colander.drop),
        name='size',
        title='Size',
        description='Size information about the resource.',
        missing=colander.drop,
    )
    version = colander.SchemaNode(
        colander.String(),
        name='version',
        title='Version',
        description='Version information about the resource.',
        missing=colander.drop,
    )
    discipline = colander.SchemaNode(
        colander.String(),
        name='discipline',
        title='Discipline',
        description='The research discipline(s) the resource can be categorized in.',
        missing=colander.drop,
    )
    spatial_coverage = colander.SchemaNode(
        colander.String(),
        name='spatial_coverage',
        title='SpatialCoverage',
        description='The spatial coverage the research data itself is related to. Content of this category is displayed in plain text. If a longitude/latitude information is given it will be displayed on the map.',  # noqa
        missing=colander.drop,
    )
    # NOTE: should enable both Date format and plain text like 'Viking age'
    temporal_coverage = colander.SchemaNode(
        colander.String(),
        name='temporal_coverage',
        title='TemporalCoverage',
        description='Period of time the research data itself is related to. Could be a date format or plain text.',  # noqa
        missing=colander.drop,
    )
