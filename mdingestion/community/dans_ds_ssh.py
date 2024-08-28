from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class Ds_Ssh(BaseDans):
    IDENTIFIER = 'ds_ssh'
    TITLE = 'DANS Data Station Social Sciences and Humanities'
    GROUP = 'dans'
    URL = 'https://ssh.datastations.nl/oai'
    PRODUCTIVE = True
    DATE = '2024-08-28'
    DESCRIPTION = 'A domain-specific repository for Social Sciences and Humanities data, primarily aimed at the Netherlands, but not exclusively.'
    LOGO = ''
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    REPOSITORY_ID = 're3data:r3d100014195'
    REPOSITORY_NAME = 'DANS Data Station Social Sciences and Humanities'
