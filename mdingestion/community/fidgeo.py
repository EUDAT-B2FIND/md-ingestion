from .base import Community
from ..service_types import SchemaType, ServiceType


class FidgeoDatacite(Community):
    NAME = 'fidgeo'
    IDENTIFIER = 'fidgeo'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.FID'
    PRODUCTIVE = True

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geospheric Sciences')
