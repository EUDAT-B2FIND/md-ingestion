from .base import Community
from ..service_types import SchemaType, ServiceType


class StarsforallDublinCore(Community):
    NAME = 'starsforall'
    IDENTIFIER = 'starsforall'
    URL = 'https://eudat-b2share-test.csc.fi/api/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = '630291ac-db47-48bf-b8aa-7acdb482c430'

    def update(self, doc):
        doc.contact = 'contact@stars4all.eu'
        #doc.contributor = ['EGI Datahub']
        #doc.pid = self.find_pid('identifier')
        #doc.source = self.source(doc)
        #doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = ['STARS4ALL']

    #def keywords(self, doc):
        #keywords = doc.keywords
        #keywords.append('EGI')
        #return keywords

    #def source(self, doc):
        #urls = [url for url in self.find('metadata.identifier') if 'handle' not in url]
        #return urls
