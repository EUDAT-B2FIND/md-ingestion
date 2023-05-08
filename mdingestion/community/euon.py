from .base import Repository
from ..service_types import SchemaType, ServiceType


class EuonDublincore(Repository):
    IDENTIFIER = 'euon'
    TITLE = 'EUON'
    GROUP = 'b2share'
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = ''  # EUON Set from CSC
    PRODUCTIVE = False
    DATE = '2023-05-08'
    REPOSITORY_ID = ''
    REPOSITORY_NAME = ''
    CRON_DAILY = False
    LOGO = "https://b2share.eudat.eu/img/communities/euon.png"
    LINK = "https://eudat.eu/about-euon"
    DESCRIPTION = """The European Ontology Network (EUON) is a community-driven, voluntary coordination between people based in Europe and working in the area of ontology and the broader area of semantics, including terminologies, vocabularies and schema. The network is intended to bring together those working across all disciplines to share their experiences, ideas and technology from both academia and industry. Members include those working in areas such as the natural sciences, physical sciences, humanities, arts, languages, social sciences, computational and mathematical sciences, but membership is open to all."""

    def update(self, doc):
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        doc.discipline = self.discipline(doc, 'Information Science')
        doc.publisher = 'EUDAT B2SHARE'
