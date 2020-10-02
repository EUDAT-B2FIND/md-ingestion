from .base import Community
from ..service_types import SchemaType, ServiceType


class LiferespiraDublinCore(Community):
    NAME = 'liferespira'
    IDENTIFIER = NAME
    URL = 'https://b2share.eudat.eu/api/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = 'e1800bc8-780e-4617-a7b6-2312cb6190c4'

    def update(self, doc):
        doc.contributor = self.contributor(doc)
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = ['Life+Respira']

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('PAIRQURS')
        return keywords

    def contributor(self, doc):
        con = doc.contributor
        con.append('B2SHARE')
        return con
