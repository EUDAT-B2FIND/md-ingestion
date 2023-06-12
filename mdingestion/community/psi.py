from .panoscexpands import BasePanoscExpands
from ..service_types import SchemaType, ServiceType


class PsiDatacite(BasePanoscExpands):
    IDENTIFIER = 'psi'
    TITLE = 'PSI'
    URL = 'https://doi.psi.ch/oaipmh/oai'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2021-04-23'
    REPOSITORY_ID = 're3data:r3d100013504'
    REPOSITORY_NAME = 'PSI - Paul Scherrer Institute'

    def update(self, doc):
        doc.discipline = self.discipline(doc, ['Life Sciences','Biology','Basic Biological and Medical Research'])
        doc.keywords = self.keywords(doc)

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('PaN')
        return keywords
