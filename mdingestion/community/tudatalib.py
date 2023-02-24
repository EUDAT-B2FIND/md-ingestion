from .base import Repository
from ..service_types import SchemaType, ServiceType


class TudatalibDatacite(Repository):
    IDENTIFIER = 'tudatalib'
    URL = 'https://tudatalib.ulb.tu-darmstadt.de/oai/openairedata'
    SCHEMA = SchemaType.DataCite
    SERVICE_TYPE = ServiceType.OAI
    OAI_METADATA_PREFIX = 'oai_datacite'
    OAI_SET = None
    PRODUCTIVE = True
    DATE = '2021-03-15'
    REPOSITORY_ID = 're3data:r3d100013029'
    REPOSITORY_NAME = 'TUdatalib'

    def update(self, doc):
        doc.contact = 'https://tudatalib.ulb.tu-darmstadt.de/page/contact'
        doc.keywords = self.keywords()

    def keywords(self):
        keywords = []
        for subject in self.reader.parser.doc.find_all('subject'):
            scheme = subject.get('subjectScheme')
            if scheme is None or scheme == "DFG":
                keywords.append(subject.text)
        return keywords
