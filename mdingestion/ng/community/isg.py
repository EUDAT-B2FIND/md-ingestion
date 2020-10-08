from .base import Community
from ..service_types import SchemaType, ServiceType


class IsgDatacite(Community):
    NAME = 'isg'
    IDENTIFIER = NAME
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.ISG'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geodesy, Geoinformatics and Remote Sensing')
