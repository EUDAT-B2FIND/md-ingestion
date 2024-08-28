from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class Ds_LifeSciences(BaseDans):
    IDENTIFIER = 'ds_lifesciences'
    TITLE = 'DANS Data Station Life Sciences'
    GROUP = 'dans'
    URL = 'https://lifesciences.datastations.nl/oai'
    PRODUCTIVE = True
    DATE = '2024-08-28'
    DESCRIPTION = 'A domain-specific repository for the Life Sciences, covering the health, medical as well as the green life sciences. The repository services are primarily aimed at the Netherlands, but not exclusively.'
    LOGO = ''
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    REPOSITORY_ID = 're3data:r3d100014304'
    REPOSITORY_NAME = 'DANS Data Station Life Sciences'
