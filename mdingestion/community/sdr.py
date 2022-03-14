from .base import Community
from ..service_types import SchemaType, ServiceType


class SDRBase(Community):
    NAME = 'sdr'
    IDENTIFIER = NAME
    URL = 'https://repository.surfsara.nl/api/oai2'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None