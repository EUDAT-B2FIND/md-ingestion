from .base import Repository
from ..service_types import SchemaType, ServiceType


class Envidat(Repository):
    IDENTIFIER = 'envidat'
    URL = 'https://www.envidat.ch'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.CKAN
    PRODUCTIVE = False
    DATE = '2023-06-12'
