from .gfz import BaseGfz
from ..service_types import SchemaType, ServiceType


class IntermagnetDatacite(BaseGfz):
    IDENTIFIER = 'intermagnet'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.INTERMAG'
    PRODUCTIVE = True

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Geophysics')