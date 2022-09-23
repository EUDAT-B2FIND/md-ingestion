from .base import Community
from ..service_types import SchemaType, ServiceType


class Edmond(Community):
    NAME = 'edmond'
    TITLE = 'Edmond'
    IDENTIFIER = NAME
    URL = 'https://demo.dataverse.org/api/search'
    SCHEMA = SchemaType.JSON
    SERVICE_TYPE = ServiceType.Dataverse
    # FILTER = ""
    PRODUCTIVE = False

    def update(self, doc):
        pass

    