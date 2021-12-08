from .base import Community
from ..service_types import SchemaType, ServiceType


class CessdaDDI25(Community):
    NAME = 'cessda'
    IDENTIFIER = NAME
    URL = 'https://datacatalogue-dev.cessda.eu/oai-pmh/v0/oai'
    SCHEMA = SchemaType.DDI25
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_ddi25'
    OAI_SET = None
#   PRODUCTIVE = True
#   DATE = '2021-10-20'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Social Sciences')
        doc.publisher = self.publisher()

    def publisher(self):
        publisher = []
        publisher.extend(self.find('distrbtr'))
        if not publisher:
            publisher.append('CESSDA')
        return publisher
