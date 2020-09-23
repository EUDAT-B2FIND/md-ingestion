from .base import Community
from ..service_types import SchemaType, ServiceType


class PangaeaDatacite(Community):
    NAME = 'pangaea'
    IDENTIFIER = 'pangaea'
    URL = 'https://ws.pangaea.de/oai/provider'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'datacite4'
    OAI_SET = None

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Earth System Research')
