from .gfz import BaseGfz
from ..service_types import SchemaType, ServiceType


class PikDatacite(BaseGfz):
    IDENTIFIER = 'pik'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.PIK'
    PRODUCTIVE = True

    def update(self, doc):
        doc.discipline = 'Earth System Research'
