from .base import Repository
from ..service_types import SchemaType, ServiceType


class GeofonDatacite(Repository):
    IDENTIFIER = 'gipp'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.GIPP'
    PRODUCTIVE = True

    def update(self, doc):
        doc.discipline = self.gipp_discipline(doc)

    def gipp_discipline(self, doc):
        if 'GIPP-MT' not in doc.doi:
            discipline = 'Seismology'
        else:
            discipline = 'Geophysics'
        return discipline
