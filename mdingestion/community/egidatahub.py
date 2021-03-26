from .base import Community
from ..service_types import SchemaType, ServiceType


class EgidatahubDublinCore(Community):
    NAME = 'egidatahub'
    IDENTIFIER = 'egidatahub'
    URL = 'http://datahub.egi.eu/oai_pmh'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True

    def update(self, doc):
        doc.contributor = ['EGI Datahub']
        doc.pid = self.find_pid('identifier')
        doc.source = self.source(doc)
        doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = ['EGI Datahub']

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('EGI')
        return keywords

    def source(self, doc):
        urls = [url for url in self.find('metadata.identifier') if 'handle' not in url]
        return urls
