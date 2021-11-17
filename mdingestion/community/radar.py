from .base import Community
from ..service_types import SchemaType, ServiceType


class Radar(Community):
    NAME = 'radar'
    IDENTIFIER = NAME
    URL = 'https://www.radar-service.eu/oai/OAIHandler'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'datacite'
    OAI_SET = None
    PRODUCTIVE = True

    def update(self, doc):
        doc.contributor = 'RADAR'
