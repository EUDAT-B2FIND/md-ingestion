from .base import Repository
from ..service_types import SchemaType, ServiceType


class CessdaDDI25(Repository):
    IDENTIFIER = 'cessda'
    URL = 'https://datacatalogue.cessda.eu/oai-pmh/v0/oai'
    SCHEMA = SchemaType.DDI25
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_ddi25'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2022-10-12'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Social Sciences')
        doc.publisher = self.publisher()

    def publisher(self):
        publisher = []
        publisher.extend(self.find('distrbtr'))
        if not publisher:
            publisher.append('CESSDA')
        return publisher
