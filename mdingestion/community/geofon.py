from .gfz import BaseGfz
from ..service_types import SchemaType, ServiceType


class GeofonDatacite(BaseGfz):
    IDENTIFIER = 'geofon'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.SEISNET'
    PRODUCTIVE = True
    REPOSITORY_ID = 're3data:r3d100011326'
    REPOSITORY_NAME = 'GEOFON Seismic Networks'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Seismology')
