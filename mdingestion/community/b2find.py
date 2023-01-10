from .base import Repository
from ..service_types import SchemaType, ServiceType


class B2FINDDatacite(Repository):
    IDENTIFIER = 'b2find'
    URL = 'http://eudatmd2.dkrz.de/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'eudat-b2find'

    def update(self, doc):
        pass
