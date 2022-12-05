from .base import Repository
from ..service_types import SchemaType, ServiceType


class EliDataCite(Repository):
    NAME = 'eli'
    IDENTIFIER = NAME
    URL = 'https://data.eli-laser.eu/oai2d'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
#   PRODUCTIVE = True
#   DATE = '2021-10-20'

    def update(self, doc):
        doc.discipline = ['Physics']
        doc.publisher = 'ELI ERIC'
        doc.source = 'http://aliceinwonderland.eu'
