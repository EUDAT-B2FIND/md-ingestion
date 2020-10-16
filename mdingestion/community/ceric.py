from .base import Community
from ..service_types import SchemaType, ServiceType


class CERICDatacite(Community):
    NAME = 'ceric'
    IDENTIFIER = 'ceric'
    URL = 'https://data.ceric-eric.eu/oai-pmh/oai2'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None

    def update(self, doc):
        doc.discipline = 'Particles, Nuclei and Fields'
        doc.open_access = True
        doc.doi = ""
        doc.source = self.find("metadata.identifier", identifierType='URL')
