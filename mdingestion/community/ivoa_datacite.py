from .base import Repository
from ..service_types import SchemaType, ServiceType


class IvoaDatacite(Repository):
    IDENTIFIER = 'ivoa_datacite'
    URL = 'http://dc.g-vo.org/rr/q/pmh/pubreg.xml'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True

    def update(self, doc):
        doc.source = self.find_source('alternateIdentifier', alternateIdentifierType="reference URL")
        doc.related_identifier = self.find('relatedIdentifier', relatedIdentifierType="bibcode")
        doc.contact = self.find('contributor', contributorType="ContactPerson")
        doc.discipline = self.discipline(doc, 'Astrophysics and Astronomy')
        doc.contributor = self.contributor(doc)

    def contributor(self, doc):
        contributor = [name for name in doc.contributor if name not in doc.contact]
        contributor.append('International Virtual Observatory Alliance (IVOA)')
        return contributor
