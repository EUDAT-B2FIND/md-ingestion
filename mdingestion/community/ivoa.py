from .base import Repository
from ..service_types import SchemaType, ServiceType


class IvoaEudatcore(Repository):
    IDENTIFIER = 'ivoa'
    URL = 'http://dc.g-vo.org/rr/q/pmh/pubreg.xml'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_b2find'
    OAI_SET = None
    PRODUCTIVE = True

    def update(self, doc):
        # doc.source = self.find_source('relatedIdentifier', relatedIdentifierType="URL")
        doc.source = self.find_source('identifier', identifierType="URL")
        doc.related_identifier = self.find('relatedIdentifier', relatedIdentifierType="bibcode")
        doc.related_identifier = self.find('relatedIdentifier', relatedIdentifierType="URL")
        doc.discipline = self.discipline(doc, 'Astrophysics and Astronomy')
        doc.contributor = self.contributor(doc)

    def contributor(self, doc):
        contributor = [name for name in doc.contributor if name not in doc.contact]
        contributor.append('International Virtual Observatory Alliance (IVOA)')
        return contributor
