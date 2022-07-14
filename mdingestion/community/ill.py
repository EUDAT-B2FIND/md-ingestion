from .panosc import BasePanosc
from ..service_types import SchemaType, ServiceType


class ILLDatacite(BasePanosc):
    NAME = 'ill'
    TITLE = 'ILL'
    IDENTIFIER = NAME
    URL = 'https://data.ill.fr/openaire/oai'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = 'openaire_data'
    PRODUCTIVE = False
#    DATE = '2022-07-14'
    DESCRIPTION = ""
    LOGO = ''

    def update(self, doc):
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.keywords = self.keywords(doc)

    def keywords(self, doc):
        keywords = doc.keywords
        keywords.append('PaN')
        return keywords
