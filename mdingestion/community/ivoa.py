from .base import Repository
from ..service_types import SchemaType, ServiceType


# TODO: filter out records with <resourceType>='Other' and <resourceType>='Text'; check validation for 'Contributor'
class IvoaEudatcore(Repository):
    IDENTIFIER = 'ivoa'
    URL = 'http://dc.g-vo.org/rr/q/pmh/pubreg.xml'
    SCHEMA = SchemaType.Eudatcore
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_b2find'
    OAI_SET = None
    PRODUCTIVE = True

    def update(self, doc):
        doc.source = self.find_source('identifier', identifierType="URL")
        doc.related_identifier = self.find('relatedIdentifier', relatedIdentifierType="bibcode")
        doc.related_identifier = self.find('relatedIdentifier', relatedIdentifierType="URL")
        doc.discipline = self.discipline(doc, 'Astrophysics and Astronomy')
        doc.contributor = self.contributor(doc)
