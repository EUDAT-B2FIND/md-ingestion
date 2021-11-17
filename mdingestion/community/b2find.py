from .base import Community
from ..service_types import SchemaType, ServiceType


class B2FINDDatacite(Community):
    NAME = 'b2find'
    IDENTIFIER = NAME
    URL = 'http://eudatmd2.dkrz.de/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'eudat-b2find'

    def update(self, doc):
        pass
