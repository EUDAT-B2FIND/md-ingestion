from .base import Community
from ..service_types import SchemaType, ServiceType


class DansEasyDatacite(Community):
    NAME = 'danseasy'
    IDENTIFIER = 'danseasy'
    URL = 'https://easy.dans.knaw.nl/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None

    def update(self, doc):
        if not doc.doi:
            doc.doi = self.find_doi('alternateIdentifier')
        if not doc.source:
            doc.source = self.find_source('alternateIdentifier')
        #doc.discipline = self.discipline(doc)
