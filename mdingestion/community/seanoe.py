from .base import Repository
from ..service_types import SchemaType, ServiceType


class Seanoe(Repository):
    IDENTIFIER = 'seanoe'
    URL = 'http://www.seanoe.org/oai/OAIHandler'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2022-02-18'
    REPOSITORY_ID = 're3data:r3d100011867'
    REPOSITORY_NAME = 'SEANOE'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Marine Science')
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'SEANOE'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('marine data')
        return keywords
