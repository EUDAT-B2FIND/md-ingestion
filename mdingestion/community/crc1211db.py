from .gfz import BaseGfz
from ..service_types import SchemaType, ServiceType


class CRC1211DBDatacite(BaseGfz):
    IDENTIFIER = 'crc1211db'
    TITLE = "CRC 1211 Database"
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.CRC1211'
    PRODUCTIVE = True
    DATE = '2020-09-23'
    REPOSITORY_ID = 're3data:r3d100013111'
    REPOSITORY_NAME = 'CRC 1211 Database'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geosciences')
        doc.contact = 'crc1211db-admin@uni-koeln.de'
