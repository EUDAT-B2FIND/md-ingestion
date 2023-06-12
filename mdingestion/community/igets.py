from .gfz import BaseGfz
from ..service_types import SchemaType, ServiceType


class IgetsDatacite(BaseGfz):
    IDENTIFIER = 'igets'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.IGETS'
    PRODUCTIVE = False
    REPOSITORY_ID = 're3data:r3d100010300'
    REPOSITORY_NAME = 'IGETS - International Geodynamics and Earth Tide Service'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geodesy, Geoinformatics and Remote Sensing')
