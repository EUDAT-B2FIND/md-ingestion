from .base import Community
from ..service_types import SchemaType, ServiceType


class GeofonDatacite(Community):
    NAME = 'gipp'
    IDENTIFIER = NAME
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.GIPP'

    def update(self, doc):
        doc.discipline = self.gipp_discipline(doc)

    def gipp_discipline(self, doc):
        if not 'GIPP-MT' in doc.doi:
            discipline = 'Seismology'
        else:
            discipline = 'Geophysics'
        return discipline
