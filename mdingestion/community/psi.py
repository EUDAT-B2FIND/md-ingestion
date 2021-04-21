from .base import Community
from ..service_types import SchemaType, ServiceType


class PsiDatacite(Community):
    NAME = 'psi'
    IDENTIFIER = NAME
    URL = 'https://doi.psi.ch/oaipmh/oai'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = False

    def update(self, doc):
        doc.discipline = self.discipline(doc, ['Life Sciences','Biology','Basic Biological and Medical Research'])
        doc.keywords = self.keywords(doc)

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('PaN')
        return keywords
