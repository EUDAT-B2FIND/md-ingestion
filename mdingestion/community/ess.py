from .base import Community
from ..service_types import SchemaType, ServiceType


class EssDatacite(Community):
    NAME = 'ess'
    IDENTIFIER = 'ess'
    URL = 'https://scicat.esss.se/openaire/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True
    # harvesting with: b2f harvest -c ess -k

    def update(self, doc):
        doc.doi = self.find_doi('identifier', identifierType="URL")
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.keywords = self.keywords(doc)
        # doc.open_access = True

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('PaN')
        return keywords
