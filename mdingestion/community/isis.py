from .base import Community
from ..service_types import SchemaType, ServiceType


class ISISDatacite(Community):
    NAME = 'isis'
    IDENTIFIER = NAME
    URL = 'https://icat-dev.isis.stfc.ac.uk/oaipmh/request'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = False
#    DATE = '2020-09-20'
