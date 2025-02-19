from .base import Repository
from ..service_types import SchemaType, ServiceType


class EnvidatDatacite(Repository):

    IDENTIFIER = 'old_envidat'
    URL = 'https://www.envidat.ch/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'datacite'
    OAI_SET = None
    PRODUCTIVE = False

    def update(self, doc):
        doc.source = self.find_source('alternateIdentifier')
        doc.contact = 'envidat@wsl.ch'
        if not doc.publisher:
            doc.publisher = 'EnviDat'
        doc.contributor = self.contributor(doc)
        doc.discipline = self.discipline(doc, 'Environmental Research')

    def contributor(self, doc):
        contributor = [name for name in doc.contributor if name not in doc.contact]
        contributor.append('EnviDat')
        return contributor
