from .base import Community
from ..service_types import SchemaType, ServiceType


class DataverseNODublinCore(Community):
    NAME = 'dataverseno'
    IDENTIFIER = 'dataverseno'
    URL = 'https://dataverse.no/oai'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = 'dataverseno'

    def update(self, doc):
        # doc.contributor = ['DataverseNO']
        doc.publication_year = self.find('header.datestamp')
        # doc.discipline = self.discipline(doc, 'Earth and Environmental Science')
        # doc.keywords = self.keywords(doc)
        if not doc.publisher:
            doc.publisher = 'DataverseNO'

    # def keywords(self, doc):
        # keywords = doc.keywords
        # keywords.append('EOSC Nordic')
        # return keywords
