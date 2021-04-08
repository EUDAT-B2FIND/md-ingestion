from .base import Community
from ..service_types import SchemaType, ServiceType


class PsiDatacite(Community):
    NAME = 'psi'
    IDENTIFIER = NAME
    URL = 'https://doi.psi.ch/'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = False

    def update(self, doc):
        doc.discipline = self.discipline(doc, ['Life Sciences','Biology','Basic Biological and Medical Research'])
