from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class ILLDatacite(Community):
    NAME = 'ill'
    IDENTIFIER = 'ill'
    URL = 'https://data.ill.fr/openaire/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'openaire_data'

    def update(self, doc):
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.keywords = self.keywords(doc)

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('PaN')
        return keywords
