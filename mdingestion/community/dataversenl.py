from .base import Repository
from ..service_types import SchemaType, ServiceType


class DataverseNLDatacite(Repository):
    IDENTIFIER = 'dataversenl'
    URL = 'https://dataverse.nl/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2021-02-23'
    REPOSITORY_ID = 're3data:r3d100011201'
    REPOSITORY_NAME = 'DataverseNL'
