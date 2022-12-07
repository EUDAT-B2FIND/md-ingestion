from .base import Repository
from ..service_types import SchemaType, ServiceType


class CRC1211DBDatacite(Repository):
    IDENTIFIER = 'crc1211db'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.CRC1211'
    PRODUCTIVE = True

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geosciences')
        doc.contact = 'crc1211db-admin@uni-koeln.de'
