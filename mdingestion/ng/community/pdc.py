from .base import Community
from ..service_types import SchemaType, ServiceType


class Pdc(Community):
    NAME = 'pdc'
    IDENTIFIER = NAME
    URL = 'http://www.polardata.ca/oai/provider'
    SCHEMA = SchemaType.FGDC
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'fgdc'
    OAI_SET = None

    def update(self, doc):
        if not doc.contributor:
            doc.contributor = 'Polar Data Catalogue'
        doc.discipline = self.discipline(doc, 'Environmental Research')
        doc.language = 'eng'
