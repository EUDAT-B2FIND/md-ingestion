from .base import Repository
from ..service_types import SchemaType, ServiceType


class CessdaDataCite(Repository):
    IDENTIFIER = 'cessda_datacite'
    URL = 'https://datacatalogue-dev.cessda.eu/oai-pmh/v0/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
#   PRODUCTIVE = True
#   DATE = '2021-10-20'
