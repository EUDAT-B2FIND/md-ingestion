from .base import Community
from ..service_types import SchemaType, ServiceType


class NIRDDublinCore(Community):
    NAME = 'nird'
    IDENTIFIER = NAME
    URL = 'https://search-api.web.sigma2.no/norstore-archive/oai/v1.0'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None

    #def update(self, doc):
        #doc.contributor = self.contributor(doc)
        #doc.keywords = self.keywords(doc)
        #if not doc.publisher:
            #doc.publisher = ['Life+Respira']

    #def keywords(self, doc):
        #keywords = doc.keywords
        #keywords.append('PAIRQURS')
        #return keywords

    #def contributor(self, doc):
        #con = doc.contributor
        #con.append('B2SHARE')
        #return con
