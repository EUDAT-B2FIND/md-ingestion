from .base import Repository
from ..service_types import SchemaType, ServiceType


class Radar(Repository):
    IDENTIFIER = 'radar'
    URL = 'https://www.radar-service.eu/oai/OAIHandler'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'datacite'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2022-09-19'

    def update(self, doc):
        doc.contributor = 'RADAR'
