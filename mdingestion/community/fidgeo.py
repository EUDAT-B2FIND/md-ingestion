from .gfz import BaseGfz
from ..service_types import SchemaType, ServiceType


class FidgeoDatacite(BaseGfz):
    IDENTIFIER = 'fidgeo'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.FID'
    PRODUCTIVE = True
    DATE = ''
    DESCRIPTION = ''
    LOGO = ''

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geospheric Sciences')
