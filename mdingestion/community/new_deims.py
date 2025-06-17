from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class Deims(Repository):
    IDENTIFIER = 'new_deims'
#    URL = 'https://deims.org/pycsw/catalogue/csw'
    URL = 'https://deims.org/pycsw/oaipmh'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'datacite'
    OAI_SET = 'datasets'
    PRODUCTIVE = True
    DATE = '2020-08-25'
    REPOSITORY_ID = 're3data:r3d100012910'
    REPOSITORY_NAME = 'DEIMS'
