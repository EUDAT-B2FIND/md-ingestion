from .gfz import BaseGfz
from ..service_types import SchemaType, ServiceType


class SddbDatacite(BaseGfz):
    IDENTIFIER = 'sddb'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.SDDB'
    PRODUCTIVE = True
    REPOSITORY_ID = 're3data:r3d100010659'
    REPOSITORY_NAME = 'Scientific Drilling Database'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Scientific drilling')
        doc.contact = 'www.icdp-online.org/contact'
