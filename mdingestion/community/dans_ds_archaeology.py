from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class Ds_Archaeology(BaseDans):
    IDENTIFIER = 'ds_archaeology'
    TITLE = 'DANS Data Station Archaeology'
    GROUP = 'dans'
    URL = 'https://archaeology.datastations.nl/oai'
    PRODUCTIVE = True
    DATE = '2024-08-28'
    DESCRIPTION = 'A domain-specific repository for Archaeology and related data, primarily aimed at the Netherlands, but not exclusively.'
    LOGO = ''
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    REPOSITORY_ID = 're3data:r3d100014005'
    REPOSITORY_NAME = 'DANS: Data Station Archaeology'
