from .base import Repository
from ..service_types import SchemaType, ServiceType


class StarsforallDublinCore(Repository):
    IDENTIFIER = 'starsforall'
    URL = 'https://eudat-b2share-test.csc.fi/api/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = '630291ac-db47-48bf-b8aa-7acdb482c430'
    PRODUCTIVE = False

    def update(self, doc):
        doc.contact = 'contact@stars4all.eu'
        if not doc.publisher:
            doc.publisher = ['STARS4ALL']

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('light pullution')
        return keywords
