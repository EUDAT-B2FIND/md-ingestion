from .base import Community
from ..service_types import SchemaType, ServiceType


class RodareDublinCore(Community):
    NAME = 'rodare'
    IDENTIFIER = NAME
    URL = 'https://rodare.hzdr.de/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = 'user-rodare'
    #PRODUCTIVE = True
    #DATE = '2021-10-20'

    def update(self, doc):
        doc.publisher = 'Rodare'
