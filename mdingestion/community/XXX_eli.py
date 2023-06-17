from .base import Repository
from ..service_types import SchemaType, ServiceType


# TODO: should be part of PaNOSC group?
class EliDataCite(Repository):
    IDENTIFIER = 'eli'
    URL = 'https://data.eli-laser.eu/oai2d'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'openaire_data'
#   PRODUCTIVE = True
#   DATE = '2021-10-20'

    def update(self, doc):
        doc.discipline = ['Physics']
        doc.publisher = 'ELI ERIC'
        doc.source = 'http://aliceinwonderland.eu'
