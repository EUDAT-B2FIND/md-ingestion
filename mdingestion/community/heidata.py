from .base import Community
from ..service_types import SchemaType, ServiceType


class heiDATADatacite(Community):
    NAME = 'heidata'
    IDENTIFIER = NAME
    URL = 'https://heidata.uni-heidelberg.de/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True
