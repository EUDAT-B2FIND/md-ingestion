from .base import Community
from ..service_types import SchemaType, ServiceType


class SDRDublinCore(Community):
    NAME = 'sdr'
    IDENTIFIER = NAME
    URL = 'https://repository.surfsara.nl/api/oai2'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
#   PRODUCTIVE = True
#   DATE = '2022-03-20'

    def update(self, doc):
        if not doc.publisher:
            doc.publisher = 'SURF Data Repository'
