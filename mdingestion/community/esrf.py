from .base import Community
from ..service_types import SchemaType, ServiceType


class ESRFDublinCore(Community):
    NAME = 'esrf'
    IDENTIFIER = 'esrf'
    URL = 'https://icatplus.esrf.fr/oaipmh/request'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None

    def update(self, doc):
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.publication_year = '2020'
        # doc.open_access = True
