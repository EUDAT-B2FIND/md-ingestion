from .base import Repository
from ..service_types import SchemaType, ServiceType
from ..format import format_value


class Deims(Repository):
    IDENTIFIER = 'deims'
    URL = 'https://deims.org/pycsw/catalogue/csw'
    SCHEMA = SchemaType.ISO19139
    SERVICE_TYPE = ServiceType.CSW
    PRODUCTIVE = True
    DATE = '2020-08-25'
    REPOSITORY_ID = 're3data:r3d100012910'
    REPOSITORY_NAME = 'DEIMS'

    # TODO: identifier check with iso
    def update(self, doc):
        if not doc.publication_year:
            doc.publication_year = self.find('header.datestamp')
        doc.doi = self.find_doi('linkage')
        doc.pid = self.find_pid('linkage')
        doc.source = self.find('MD_Identifier')
        doc.contributor = 'DEIMS-SDR Site and Dataset registry deims.org'
        doc.discipline = 'Environmental Monitoring'
        if not doc.publisher:
            doc.publisher = 'DEIMS-SDR'
        doc.discipline = self.discipline(doc, 'Environmental Monitoring')
        self.fix_source(doc)

    def fix_source(self, doc):
        if not doc.source:
            source = format_value(self.find('MD_Identifier'), one=True)
            if 'xlink' in source:
                doc.source = source.split()[1]
        if doc.source.startswith("https://deims.org/datasets/"):
            doc.source = doc.source.replace("https://deims.org/datasets/", "https://deims.org/dataset/")
