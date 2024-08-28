from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class Ds_Phys_Tech(BaseDans):
    IDENTIFIER = 'ds_physical_technical'
    TITLE = 'DANS Data Station Physical and Technical Sciences'
    GROUP = 'dans'
    URL = 'https://phys-techsciences.datastations.nl/oai'
    PRODUCTIVE = True
    DATE = '2024-08-28'
    DESCRIPTION = 'A domain-specific repository for the physical and technical sciences. The repository services are primarily aimed at the Netherlands, but not exclusively.'
    LOGO = ''
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
#    REPOSITORY_ID = 're3data:'
#    REPOSITORY_NAME = 'DANS Data Station Physical and Technical Sciences'
