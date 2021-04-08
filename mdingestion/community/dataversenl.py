from .base import Community
from ..service_types import SchemaType, ServiceType


class DataverseNLDatacite(Community):
    NAME = 'dataversenl'
    IDENTIFIER = NAME
    URL = 'https://dataverse.nl/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True
