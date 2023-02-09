from .base import Repository
from ..service_types import SchemaType, ServiceType


class Toar(Repository):
    IDENTIFIER = 'toar'
    # URL = 'https://b2share.fz-juelich.de/api/oai2d'
    URL = 'https://b2share-testing.fz-juelich.de/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = '26e8202d-7094-412d-be28-7e64bf6ac77f'
    PRODUCTIVE = False

# TODO: add 'Group' B2SHARE
#     def update(self, doc):
#        doc.contributor = 'B2SHARE'
#        doc.pid = self.find_pid('identifier')
#        doc.source = self.source(doc)
#        doc.keywords = self.keywords(doc)
#        doc.publisher = 'Tropospheric Ozone Assessment Report (TOAR)'
#        doc.discipline = 'Atmospheric Chemistry'

#    def source(self, doc):
#        urls = [url for url in self.find('metadata.identifier') if 'handle' not in url]
#        return urls
