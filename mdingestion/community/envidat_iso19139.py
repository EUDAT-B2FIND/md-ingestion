from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class Envidat(Repository):
    # NAME = 'envidat_iso'
    IDENTIFIER = 'envidat_iso19139'
    URL = 'https://www.envidat.ch/oai'
    SCHEMA = SchemaType.ISO19139
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'iso19139'
    OAI_SET = None

    def update(self, doc):
        doc.doi = self.find_doi('MD_Metadata.fileIdentifier')
        doc.pid = self.find_pid('MD_Metadata.fileIdentifier')
        doc.source = self.find_source('linkage')
        doc.discipline = self.discipline(doc, 'Environmental Research')
