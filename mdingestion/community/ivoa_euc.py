from .base import Community
from ..service_types import SchemaType, ServiceType


class IvoaEudatcore(Community):
    NAME = 'ivoa'
    IDENTIFIER = 'ivoa'
    URL = 'http://dc.g-vo.org/rr/q/pmh/pubreg.xml'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_b2find'
    OAI_SET = None
    PRODUCTIVE = False

    def update(self, doc):
        doc.source = self.find_source('relatedIdentifier', relatedIdentifierType="URL")
        #doc.related_identifier = self.find('relatedIdentifier', relatedIdentifierType="bibcode")
        #doc.contact = self.find('contributor', contributorType="ContactPerson")
        doc.discipline = self.discipline(doc, 'Astrophysics and Astronomy')
        doc.contributor = self.contributor(doc)
        #doc.contact = self.contact(doc)

    def contributor(self, doc):
        contributor = [name for name in doc.contributor if name not in doc.contact]
        contributor.append('International Virtual Observatory Alliance (IVOA)')
        return contributor
