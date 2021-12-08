from .base import Community
from ..service_types import SchemaType, ServiceType


class SciencesPoDublinCore(Community):
    NAME = 'sciencespodc'
    IDENTIFIER = NAME
    URL = 'https://data.sciencespo.fr/oai'
    SCHEMA = SchemaType.DublinCore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_dc'
    OAI_SET = None
#   PRODUCTIVE = True
#   DATE = '2021-10-20'

    def update(self, doc):
        doc.discipline = self.discipline(doc, 'Social Sciences')
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
