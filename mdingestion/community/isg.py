from .gfz import BaseGfz
from ..service_types import SchemaType, ServiceType


class IsgDatacite(BaseGfz):
    IDENTIFIER = 'isg'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.ISG'
    PRODUCTIVE = True
    REPOSITORY_ID = 're3data:r3d100011289'
    REPOSITORY_NAME = 'ISG'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geodesy, Geoinformatics and Remote Sensing')
