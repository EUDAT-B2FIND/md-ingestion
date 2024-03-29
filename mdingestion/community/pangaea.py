from .base import Repository
from ..service_types import SchemaType, ServiceType


class PangaeaDatacite(Repository):
    IDENTIFIER = 'pangaea'
    URL = 'https://ws.pangaea.de/oai/provider'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'datacite4'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2020-09-30'
    REPOSITORY_ID = 're3data:r3d100010134'
    REPOSITORY_NAME = 'PANGAEA'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Earth System Research')
