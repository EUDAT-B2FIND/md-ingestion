from .base import Repository
from ..service_types import SchemaType, ServiceType


class Toar(Repository):
    IDENTIFIER = 'toar'
    URL = 'https://b2share.fz-juelich.de/api/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = '381a24f1-18d4-405d-af36-c76ba199a754'

    def update(self, doc):
        doc.contributor = 'B2SHARE'
        # doc.pid = self.find_pid('identifier')
        # doc.source = self.source(doc)
        # doc.keywords = self.keywords(doc)
        doc.publisher = 'Tropospheric Ozone Assessment Report (TOAR)'
        doc.discipline = 'Atmospheric Chemistry'

    def source(self, doc):
        urls = [url for url in self.find('metadata.identifier') if 'handle' not in url]
        return urls
