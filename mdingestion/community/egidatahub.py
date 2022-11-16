from .base import Community
from ..service_types import SchemaType, ServiceType


class EgidatahubDublinCore(Community):
    NAME = 'egidatahub'
    IDENTIFIER = NAME
    URL = 'http://datahub.egi.eu/oai_pmh'
    OAI_SET = 'eeaa135fa0822240a3cd4ac2ba5ce1fb'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
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
