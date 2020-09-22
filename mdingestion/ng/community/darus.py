from .base import Community
from .._types import SchemaType
from ..harvester import ServiceType


class DarusDatacite(Community):
    NAME = 'darus'
    IDENTIFIER = 'darus'
    URL = 'https://darus.uni-stuttgart.de/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
