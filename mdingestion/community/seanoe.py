from .base import Community
from ..service_types import SchemaType, ServiceType


class Seanoe(Community):
    NAME = 'seanoe'
    IDENTIFIER = NAME
    URL = 'http://www.seanoe.org/oai/OAIHandler'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True

    def update(self, doc):
#        doc.related_identifier = self.find('relation')
        doc.discipline = self.discipline(doc, 'Marine Science')
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'SEANOE'

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('marine data')
        return keywords
