from .gfz import BaseGfz
from ..service_types import SchemaType, ServiceType


class EnmapDatacite(BaseGfz):
    IDENTIFIER = 'enmap'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.ENMAP'
    PRODUCTIVE = True

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geodesy, Geoinformatics and Remote Sensing')
        # doc.funding_reference = self.find('Funder')
