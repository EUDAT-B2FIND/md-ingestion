from .base import Community
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class Deims(Community):
    NAME = 'deims'
    IDENTIFIER = 'deims'
    URL = 'https://deims.org/pycsw/catalogue/csw'
    SCHEMA = SchemaType.ISO19139
    SERVICE_TYPE = ServiceType.CSW

    def update(self, doc):
        doc.doi = self.find_doi('linkage')
        doc.pid = self.find_pid('linkage')
        doc.source = self.find('MD_Identifier')
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'
        # TODO: why do we not use csw-metadataccess? It points to dc metadata though...fix it, Carsten!
        doc.metadata_access = [url for url in self.find('linkage') if 'deims.org/api/' in url]
        doc.discipline = self.discipline(doc, 'Environmental Monitoring')
        self.fix_source(doc)

    def fix_source(self, doc):
        if not doc.source:
            source = format_value(self.find('MD_Identifier'), one=True)
            if 'xlink' in source:
                doc.source = source.split()[1]
        if doc.source.startswith("https://deims.org/datasets/"):
            doc.source = doc.source.replace("https://deims.org/datasets/", "https://deims.org/dataset/")
