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

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Earth System Research')
        # TODO: waiting for feedback
        # doc.contact = 'something@pangaea.de'
