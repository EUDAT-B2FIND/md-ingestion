from .base import Community
from ..service_types import SchemaType, ServiceType


class GeofonDatacite(Community):
    NAME = 'geofon'
    IDENTIFIER = 'geofon'
    URL = 'http://doidb.wdc-terra.org/oaip/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'DOIDB.GEOFON'

    def update(self, doc):
        doc.discipline = 'Seismology'
