from .base import Repository
from ..service_types import SchemaType, ServiceType


class heiDATADatacite(Repository):
    IDENTIFIER = 'heidata'
    URL = 'https://heidata.uni-heidelberg.de/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2021-03-23'
    REPOSITORY_ID = 're3data:r3d100011108'
    REPOSITORY_NAME = 'heiDATA'
