from .base import Repository
from ..service_types import SchemaType, ServiceType


class NIRDDublinCore(Repository):
    IDENTIFIER = 'nird'
    URL = 'https://search-api.web.sigma2.no/norstore-archive/oai/v1.0'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2021-07-01'
    REPOSITORY_ID = 're3data:r3d100011678'
    REPOSITORY_NAME = 'NIRD'

    def update(self, doc):
        doc.keywords = self.cleankeywords(doc)
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')

    def cleankeywords(self, doc):
        newkeywords = []
        for keyword in doc.keywords:
            if 'subfield' in keyword:
                newkeywords.append(keyword.split('subfield')[1])
        return newkeywords
