from .base import Community
from ..service_types import SchemaType, ServiceType


class FMI(Community):
    NAME = 'fmi'
    IDENTIFIER = NAME
    URL = 'https://fmi.b2share.csc.fi/api/oai2d'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = '8343aaf9-f598-4529-9359-7ce2d9bd3e63'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Meteorology')
        doc.publisher = 'Finnish Meteorological Institute'
        # doc.funding_reference = self.find('Funder')
