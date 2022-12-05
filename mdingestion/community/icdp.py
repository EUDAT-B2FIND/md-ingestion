from .base import Repository
from ..service_types import SchemaType, ServiceType


class IcdpDatacite(Repository):
    NAME = 'icdp'
    IDENTIFIER = NAME
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.SDDB'
    PRODUCTIVE = True

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Scientific drilling')
        doc.contact = 'www.icdp-online.org/contact'
