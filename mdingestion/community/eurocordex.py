from .base import Repository
from ..service_types import SchemaType, ServiceType


class EUROCordex(Repository):
    NAME = 'euro-cordex'
    TITLE = 'EURO-CORDEX'
    IDENTIFIER = NAME
    URL = 'https://b2share.fz-juelich.de/api/oai2d'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'eudatcore'
    OAI_SET = 'a140d3f3-0117-4665-9945-4c7fcb9afb51'
    
    PRODUCTIVE = False
    DATE = ''
    DESCRIPTION = ''
    LOGO = ''
